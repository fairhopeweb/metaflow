import hashlib
import json
import os
import random
import shlex
import string
import sys
import time
import uuid
from collections import defaultdict

from metaflow.decorators import flow_decorators
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    BATCH_METADATA_SERVICE_HEADERS,
    BATCH_METADATA_SERVICE_URL,
    DATASTORE_CARD_S3ROOT,
    DATASTORE_LOCAL_DIR,
    DATASTORE_SYSROOT_S3,
    DATATOOLS_S3ROOT,
    DEFAULT_METADATA,
)
from metaflow.mflog import (
    BASH_SAVE_LOGS,
    export_mflog_env_vars,
)
from metaflow.parameters import deploy_time_eval
from metaflow.util import compress_list, dict_to_cli_options

from .argo_client import ArgoClient


class ArgoWorkflowsException(MetaflowException):
    headline = "Argo Workflows error"


class ArgoWorkflowsSchedulingException(MetaflowException):
    headline = "Argo Workflows scheduling error"


# List of future enhancements -
#     1. Ensure that failed steps are retried on a different host.
#     2. Make Argo Workflows log archival optional.
#     3. Configure Argo metrics.
#     4. Support Argo Events.
#     5. Add annotations when workflow is triggered via metaflow.
#
#
#
# List of fixes before release -
#     1. Stable task ids when tasks fail.
#     2. Test for all Argo versions.
#     3. All kubernetes resource names are compatible.
#     4. Support parameters.

class ArgoWorkflows(object):
    def __init__(
        self,
        name,
        graph,
        flow,
        code_package_sha,
        code_package_url,
        production_token,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags=None,
        namespace=None,
        username=None,
        max_workers=None,
        workflow_timeout=None,
        is_project=False,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.code_package_sha = code_package_sha
        self.code_package_url = code_package_url
        self.production_token = production_token
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags
        self.namespace = namespace
        self.username = username
        self.max_workers = max_workers
        self.workflow_timeout = workflow_timeout

        self._workflow_template = self._compile()
        self._cron = self._cron()

    def __str__(self):
        return str(self._workflow_template)

    def deploy(self):
        try:
            # TODO: Set Kubernetes namespace
            ArgoClient().register_workflow_template(
                self.name, self._workflow_template.to_json()
            )
        except Exception as e:
            raise ArgoWorkflowsException(str(e))

    @classmethod
    def trigger(cls, name, parameters={}):
        try:
            # TODO: Set Kubernetes namespace
            # TODO: Check that the workflow was deployed through Metaflow
            workflow_template = ArgoClient().get_workflow_template(name)
        except Exception as e:
            raise ArgoWorkflowsException(str(e))
        if workflow_template is None:
            raise ArgoWorkflowsException(
                "The workflow *%s* doesn't exist on Argo Workflows. Please "
                "deploy your flow first." % name
            )
        try:
            return ArgoClient().trigger_workflow_template(name, parameters)
        except Exception as e:
            raise ArgoWorkflowsException(str(e))

    def _cron(self):
        schedule = self.flow._flow_decorators.get("schedule")
        if schedule:
            # Remove the field "Year" if it exists
            return " ".join(schedule.schedule.split()[:5])
        return None

    def schedule(self):
        try:
            ArgoClient().schedule_workflow_template(self.name, self._cron)
        except Exception as e:
            raise ArgoWorkflowsSchedulingException(str(e))

    def trigger_explanation(self):
        if self._cron:
            return (
                "This workflow triggers automatically via the CronWorkflow *%s*."
                % self.name
            )
        else:
            return "No triggers defined. You need to launch this workflow manually."

    @classmethod
    def get_existing_deployment(cls, name):
        workflow_template = ArgoClient().get_workflow_template(name)
        if workflow_template is not None:
            try:
                return (
                    workflow_template["metadata"]["annotations"]["metaflow/owner"],
                    workflow_template["metadata"]["annotations"][
                        "metaflow/production_token"
                    ],
                )
            except KeyError as e:
                raise ArgoWorkflowsException(
                    "An existing non-metaflow workflow with the same name as "
                    "*%s* already exists in Argo Workflows. \nPlease modify the "
                    "name of this flow or delete your existing workflow on Argo "
                    "Workflows." % name
                )
        return None

    def _process_parameters(self):
        parameters = []
        has_schedule = self._cron() is not None
        seen = set()
        for var, param in self.flow._get_parameters():
            # Throw an exception if the parameter is specified twice.
            norm = param.name.lower()
            if norm in seen:
                raise MetaflowException(
                    "Parameter *%s* is specified twice. "
                    "Note that parameter names are "
                    "case-insensitive." % param.name
                )
            seen.add(norm)

            is_required = param.kwargs.get("required", False)
            # Throw an exception if a schedule is set for a flow with required
            # parameters with no defaults. We currently don't have any notion
            # of data triggers in Argo Workflows.
            # TODO: Support Argo Events for data triggering.
            if "default" not in param.kwargs and is_required and has_schedule:
                raise MetaflowException(
                    "The parameter *%s* does not have a default and is required. "
                    "Scheduling such parameters via Argo CronWorkflows is not "
                    "currently supported." % param.name
                )
            value = deploy_time_eval(param.kwargs.get("default"))
            parameters.append(dict(name=param.name, value=value))
        return parameters

    def _compile(self):
        # This method compiles a Metaflow FlowSpec into Argo WorkflowTemplate
        #
        # WorkflowTemplate
        #   |
        #    -- WorkflowSpec
        #         |
        #          -- Array<Template>
        #                     |
        #                      -- DAGTemplate, ContainerTemplate
        #                           |                  |
        #                            -- Array<DAGTask> |
        #                                       |      |
        #                                        -- Template
        #
        # Steps in FlowSpec are represented as DAGTasks.
        # A DAGTask can reference to -
        #     a ContainerTemplate (for linear steps..) or
        #     another DAGTemplate (for nested foreaches).
        #
        # While we could have very well inlined container templates inside a DAGTask,
        # unfortunately Argo variable substitution ({{pod.name}}) doesn't work as
        # expected within DAGTasks
        # (https://github.com/argoproj/argo-workflows/issues/7432) and we are forced to
        # generate container templates at the top level (in WorkflowSpec) and maintain
        # references to them within the DAGTask.
        #
        # A note on fail-fast behavior for Argo Workflows - Argo stops
        # scheduling new steps as soon as it detects that one of the DAG nodes
        # has failed. After waiting for all the scheduled DAG nodes to run till
        # completion, Argo with fail the DAG. This implies that after a node
        # has failed, it may be a while before the entire DAG is marked as
        # failed. There is nothing Metaflow can do here for failing even
        # faster (as of Argo 3.2).

        try:
            # Kubernetes is a soft dependency for generating Argo objects.
            # We can very well remove this dependency for Argo with the downside of
            # adding a bunch more json bloat classes (looking at you... V1Container)
            from kubernetes import client as kubernetes_sdk
        except (NameError, ImportError):
            raise MetaflowException(
                "Could not import Python package 'kubernetes'. Install kubernetes "
                "sdk (https://pypi.org/project/kubernetes/) first."
            )

        # Visit every node and yield the uber DAGTemplate(s).
        def _to_dag_templates(graph):
            yield (
                Template(self.name)
                .dag(
                    DAGTemplate()
                    .fail_fast()
                    .tasks(
                        [
                            DAGTask(node.name)
                            .dependencies(node.in_funcs)
                            .template(node.name)
                            .arguments(
                                # Set input-paths.
                                Arguments().parameters(
                                    [
                                        Parameter("input-paths").value(
                                            "argo-{{workflow.name}}/_parameters/0-params"
                                            if node.name == "start"
                                            else compress_list(
                                                [
                                                    "argo-{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}"
                                                    % (2 * (n,))
                                                    for n in node.in_funcs
                                                ]
                                            )
                                        )
                                    ]
                                )
                            )
                            for node in graph
                        ]
                    )
                )
            )

        # Visit every node and yield ContainerTemplates.
        def _to_container_templates(graph):

            for node in self.graph:
                # Resolve entry point for pod container.
                script_name = os.path.basename(sys.argv[0])
                executable = self.environment.executable(node.name)
                # TODO: Support R someday. Quite a few people will be happy.
                entrypoint = [executable, script_name]

                # The values with curly braces '{{}}' are made available by Argo.
                # Unfortunately, there are a few bugs in Argo which prevent
                # us from accessing these valuesas liberally as we would like to.
                run_id = "argo-{{workflow.name}}"
                # TODO: Ensure pod names are first class citizens in @kubernetes.
                #       Currently, pod ids enjoy that honor.
                task_id = "{{pod.name}}"

                # Resolve retry strategy.
                (
                    user_code_retries,
                    total_retries,
                    retry_count,
                    minutes_between_retries,
                ) = self._get_retries(node)

                mflog_expr = export_mflog_env_vars(
                    datastore_type=self.flow_datastore.TYPE,
                    stdout_path="$PWD/.logs/mflog_stdout",
                    stderr_path="$PWD/.logs/mflog_stderr",
                    flow_name=self.flow.name,
                    run_id=run_id,
                    step_name=node.name,
                    task_id=task_id,
                    retry_count=retry_count,
                )

                cmds = (
                    [
                        "mkdir -p $PWD/.logs",
                        mflog_expr,
                        "printenv",
                    ]
                    + self.environment.get_package_commands(self.code_package_url)
                    + self.environment.bootstrap_commands(node.name)
                )

                input_paths = "{{inputs.parameters.input-paths}}"

                top_opts_dict = {
                    "with": [
                        decorator.make_decorator_spec()
                        for decorator in node.decorators
                        if not decorator.statically_defined
                    ]
                }
                # FlowDecorators can define their own top-level options. They are
                # responsible for adding their own top-level options and values through
                # the get_top_level_options() hook. See similar logic in runtime.py.
                for deco in flow_decorators():
                    top_opts_dict.update(deco.get_top_level_options())

                top_level = list(dict_to_cli_options(top_opts_dict)) + [
                    "--quiet",
                    "--metadata=%s" % self.metadata.TYPE,
                    "--environment=%s" % self.environment.TYPE,
                    "--datastore=%s" % self.flow_datastore.TYPE,
                    "--datastore-root=%s" % self.flow_datastore.datastore_root,
                    "--event-logger=%s" % self.event_logger.logger_type,
                    "--monitor=%s" % self.monitor.monitor_type,
                    "--no-pylint",
                    # "--with=argo_internal",
                ]

                # TODO: Handle foreach/branching/joins

                if node.name == "start":
                    task_id_params = "%s-params" % task_id
                    init = (
                        entrypoint
                        + top_level
                        + [
                            "init",
                            "--run-id %s" % run_id,
                            "--task-id %s" % task_id_params,
                        ]
                    )
                    init.extend(
                        [
                            "--%s={{workflow.parameters.%s}}"
                            % (parameter["name"], parameter["name"])
                            for parameter in self._process_parameters()
                        ]
                    )
                    if self.tags:
                        init.extend("--tag %s" % tag for tag in self.tags)
                    cmds.extend([" ".join(init)])
                    input_paths = "%s/_parameters/%s" % (run_id, task_id_params)

                step = [
                    "step",
                    node.name,
                    "--run-id %s" % run_id,
                    "--task-id %s" % task_id,
                    "--retry-count %s" % retry_count,
                    "--max-user-code-retries %d" % user_code_retries,
                    "--input-paths %s" % input_paths,
                ]

                if self.tags:
                    step.extend("--tag %s" % tag for tag in self.tags)
                if self.namespace is not None:
                    step.append("--namespace=%s" % self.namespace)

                cmd_str = "%s; c=$?; %s; exit $c" % (
                    " && ".join(cmds + [" ".join(entrypoint + top_level + step)]),
                    BASH_SAVE_LOGS,
                )
                cmds = shlex.split('bash -c "%s"' % cmd_str)

                # Resolve resource requirements.
                # TODO: Fix compute_resource_attributes nonsense thats strewn everywhere
                resources = dict(
                    [deco for deco in node.decorators if deco.name == "kubernetes"][
                        0
                    ].attributes
                )

                run_time_limit = [
                    deco for deco in node.decorators if deco.name == "kubernetes"
                ][0].run_time_limit

                # Resolve @environment decorator.
                env = dict(
                    [deco for deco in node.decorators if deco.name == "environment"][
                        0
                    ].attributes["vars"]
                )
                env.update(
                    {
                        **{
                            # These values are needed by Metaflow to set it's internal
                            # state appropriately
                            "METAFLOW_CODE_URL": self.code_package_url,
                            "METAFLOW_CODE_SHA": self.code_package_sha,
                            "METAFLOW_CODE_DS": self.flow_datastore.TYPE,
                            "METAFLOW_SERVICE_URL": BATCH_METADATA_SERVICE_URL,
                            "METAFLOW_SERVICE_HEADERS": json.dumps(
                                BATCH_METADATA_SERVICE_HEADERS
                            ),
                            "METAFLOW_DATASTORE_SYSROOT_S3": DATASTORE_SYSROOT_S3,
                            "METAFLOW_DATATOOLS_S3ROOT": DATATOOLS_S3ROOT,
                            "METAFLOW_DEFAULT_DATASTORE": self.flow_datastore.TYPE,
                            "METAFLOW_DEFAULT_METADATA": DEFAULT_METADATA,
                            "METAFLOW_CARD_S3ROOT": DATASTORE_CARD_S3ROOT,
                            "METAFLOW_KUBERNETES_WORKLOAD": 1,
                            "METAFLOW_RUNTIME_ENVIRONMENT": "kubernetes",
                            "METAFLOW_OWNER": self.username,
                        },
                        **{
                            # Some optional values for bookkeeping
                            "METAFLOW_FLOW_NAME": self.flow.name,
                            "METAFLOW_STEP_NAME": node.name,
                            "METAFLOW_RUN_ID": run_id,
                            "METAFLOW_TASK_ID": task_id,
                            "METAFLOW_RETRY_COUNT": retry_count,
                            "METAFLOW_PRODUCTION_TOKEN": self.production_token,
                            "ARGO_WORKFLOW_NAME": self.name,
                        },
                        **self.metadata.get_runtime_environment("argo-workflows"),
                    }
                )
                metaflow_version = self.environment.get_environment_info()
                metaflow_version["flow_name"] = self.graph.name
                metaflow_version["production_token"] = self.production_token
                env["METAFLOW_VERSION"] = json.dumps(metaflow_version)

                yield (
                    Template(node.name)
                    # Set @timeout values
                    .active_deadline_seconds(run_time_limit)
                    # TODO: Correct this before release
                    .service_account_name("s3-full-access")
                    .inputs(Inputs().parameters([Parameter("input-paths")]))
                    .outputs(
                        Outputs().parameters(
                            [Parameter("task-id").value("{{pod.name}}")]
                        )
                    )
                    .fail_fast()
                    .retry_strategy(
                        times=total_retries,
                        minutes_between_retries=minutes_between_retries,
                    )
                    # .metadata(...)
                    .container(
                        kubernetes_sdk.V1Container(
                            name=node.name,
                            command=[cmds[0]],
                            # TODO: Roll args into command(?)
                            args=cmds[1:],
                            env=[
                                kubernetes_sdk.V1EnvVar(name=k, value=str(v))
                                for k, v in env.items()
                            ]
                            # TODO: Get this working.
                            #       And some downward API magic. Add (key, value)
                            #       pairs below to make pod metadata available
                            #       within Kubernetes container.
                            + [
                                kubernetes_sdk.V1EnvVar(
                                    name=k,
                                    value_from=kubernetes_sdk.V1EnvVarSource(
                                        field_ref=kubernetes_sdk.V1ObjectFieldSelector(
                                            field_path=str(v)
                                        )
                                    ),
                                )
                                for k, v in {
                                    "METAFLOW_KUBERNETES_POD_NAMESPACE": "metadata.namespace",
                                    "METAFLOW_KUBERNETES_POD_NAME": "metadata.name",
                                    "METAFLOW_KUBERNETES_POD_ID": "metadata.uid",
                                }.items()
                            ],
                            # env_from=...,
                            image=resources["image"],
                            resources=kubernetes_sdk.V1ResourceRequirements(
                                # TODO - Consider adding limits as well.
                                requests={
                                    # TODO: Address GPUs and all other @kubernetes
                                    #       params.
                                    "cpu": str(resources["cpu"]),
                                    "memory": "%sM" % str(resources["memory"]),
                                    "ephemeral-storage": "%sM" % str(resources["disk"]),
                                }
                            ),
                        )
                    )
                )

        return (
            WorkflowTemplate()
            .metadata(
                # Workflow level metadata.
                ObjectMeta()
                .name(self.name)
                .namespace("default")
                .label("app", "metaflow")
                .label("metaflow/flow_name", self.flow.name)
                .label("metaflow/owner", self.username)
                # TODO: Add labels/annotations for project and branch
                .annotation("metaflow/production_token", self.production_token)
                .annotation("metaflow/owner", self.username)
            )
            .spec(
                WorkflowSpec()
                # Set overall workflow timeout.
                .active_deadline_seconds(self.workflow_timeout)
                # Allow Argo to archive all workflow execution logs
                .archive_logs()
                # .service_account_name(...)
                # Limit workflow parallelism
                .parallelism(self.max_workers)
                # Handle parameters
                .arguments(
                    Arguments().parameters(
                        [
                            Parameter(parameter["name"]).value(parameter["value"])
                            for parameter in self._process_parameters()
                        ]
                    )
                )
                # TODO: Set common pod metadata for all tasks.
                # .pod_metadata(...)
                # Set the entrypoint to flow name
                .entrypoint(self.name)
                # Top-level DAG template(s)
                .templates(_to_dag_templates(self.graph))
                # Container templates
                .templates(_to_container_templates(self.graph))
            )
        )

    def _get_retries(self, node):
        max_user_code_retries = 0
        max_error_retries = 0
        minutes_between_retries = "2"
        for deco in node.decorators:
            if deco.name == "retry":
                minutes_between_retries = deco.attributes.get(
                    "minutes_between_retries", minutes_between_retries
                )
            user_code_retries, error_retries = deco.step_task_retry_count()
            max_user_code_retries = max(max_user_code_retries, user_code_retries)
            max_error_retries = max(max_error_retries, error_retries)

        return (
            max_user_code_retries,
            max_user_code_retries + max_error_retries,
            # {{retries}} is only available if retryStrategy is specified
            "{{retries}}" if max_user_code_retries + max_error_retries else 0,
            int(minutes_between_retries),
        )


# Helper classes to assist with JSON-foo. This can very well replaced with an explicit
# dependency on argo-workflows Python SDK if this method turns out to be painful.
# TODO: Autogenerate them, maybe?


class WorkflowTemplate(object):
    # https://argoproj.github.io/argo-workflows/fields/#workflowtemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["apiVersion"] = "argoproj.io/v1alpha1"
        self.payload["kind"] = "WorkflowTemplate"

    def metadata(self, object_meta):
        self.payload["metadata"] = object_meta.to_json()
        return self

    def spec(self, workflow_spec):
        self.payload["spec"] = workflow_spec.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class ObjectMeta(object):
    # https://argoproj.github.io/argo-workflows/fields/#objectmeta

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)

    def generate_name(self, generate_name):
        self.payload["generateName"] = generate_name
        return self

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels)

    def name(self, name):
        self.payload["name"] = name
        return self

    def namespace(self, namespace):
        self.payload["namespace"] = namespace
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class WorkflowSpec(object):
    # https://argoproj.github.io/argo-workflows/fields/#workflowspec
    # This object sets all Workflow level properties.

    # TODO: Handle arguments, automountServiceToken, imagePullSecrets, metrics,
    #       nodeSelector, priority, retryStrategy, schedulerName, serviceAccountName,
    #       tolerations, volumes, workflowMetadata, workflowTemplateRef, ttlStrategy,
    #       affinity - certain elements are applied at WorkflowTemplateSpec level and
    #       certain others are applied at Template level

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def active_deadline_seconds(self, active_deadline_seconds):
        # Overall duration of a workflow in seconds
        if active_deadline_seconds is not None:
            self.payload["activeDeadlineSeconds"] = int(active_deadline_seconds)
        return self

    def arguments(self, arguments):
        self.payload["arguments"] = arguments.to_json()
        return self

    def archive_logs(self, archive_logs=True):
        self.payload["archiveLogs"] = archive_logs
        return self

    def entrypoint(self, entrypoint):
        self.payload["entrypoint"] = entrypoint
        return self

    def parallelism(self, parallelism):
        # Set parallelism at Workflow level
        self.payload["parallelism"] = int(parallelism)
        return self

    def pod_metadata(self, metadata):
        self.payload["podMetadata"] = metadata.to_json()
        return self

    def service_account_name(self, service_account_name):
        # https://argoproj.github.io/argo-workflows/workflow-rbac/
        self.payload["serviceAccountName"] = service_account_name
        return self

    def templates(self, templates):
        if "templates" not in self.payload:
            self.payload["templates"] = []
        for template in templates:
            self.payload["templates"].append(template.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class Metadata(object):
    # https://argoproj.github.io/argo-workflows/fields/#metadata

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def annotation(self, key, value):
        self.payload["annotations"][key] = str(value)
        return self

    def annotations(self, annotations):
        if "annotations" not in self.payload:
            self.payload["annotations"] = {}
        self.payload["annotations"].update(annotations)

    def label(self, key, value):
        self.payload["labels"][key] = str(value)
        return self

    def labels(self, labels):
        if "labels" not in self.payload:
            self.payload["labels"] = {}
        self.payload["labels"].update(labels)

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.to_json(), indent=4)


class Template(object):
    # https://argoproj.github.io/argo-workflows/fields/#template

    # TODO: Handle affinity, automountServiceAccountToken, nodeSelector, inputs,
    #       outputs, priority, retryStrategy, serviceAccountName, tolerations -
    #       certain elements are applied at WorkflowTemplateSpec level and
    #       certain others are applied at Template level
    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def active_deadline_seconds(self, active_deadline_seconds):
        # Overall duration of a pod in seconds, only obeyed for container templates
        # Used for implementing @timeout.
        self.payload["activeDeadlineSeconds"] = int(active_deadline_seconds)
        return self

    def dag(self, dag_template):
        self.payload["dag"] = dag_template.to_json()
        return self

    def container(self, container):
        # Lucklily this can simply be V1Container and we are spared from writing more
        # boilerplate - https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md.
        self.payload["container"] = container.to_dict()
        return self

    def inputs(self, inputs):
        self.payload["inputs"] = inputs.to_json()
        return self

    def outputs(self, outputs):
        self.payload["outputs"] = outputs.to_json()
        return self

    def fail_fast(self, fail_fast=True):
        # https://github.com/argoproj/argo-workflows/issues/1442
        self.payload["failFast"] = fail_fast
        return self

    def metadata(self, metadata):
        self.payload["metadata"] = metadata.to_json()
        return self

    def service_account_name(self, service_account_name):
        self.payload["serviceAccountName"] = service_account_name
        return self

    def retry_strategy(self, times, minutes_between_retries):
        if times > 0:
            self.payload["retryStrategy"] = {
                "retryPolicy": "Always",
                "limit": times,
                "backoff": {"duration": "%sm" % minutes_between_retries},
            }
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Inputs(object):
    # https://argoproj.github.io/argo-workflows/fields/#inputs

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Outputs(object):
    # https://argoproj.github.io/argo-workflows/fields/#outputs

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Parameter(object):
    # https://argoproj.github.io/argo-workflows/fields/#parameter

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def value(self, value):
        self.payload["value"] = value
        return self

    def valueFrom(self, value_from):
        self.payload["valueFrom"] = value_from
        return self

    def description(self, description):
        self.payload["description"] = description
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class DAGTemplate(object):
    # https://argoproj.github.io/argo-workflows/fields/#dagtemplate

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def fail_fast(self, fail_fast=True):
        # https://github.com/argoproj/argo-workflows/issues/1442
        self.payload["failFast"] = fail_fast
        return self

    def tasks(self, tasks):
        if "tasks" not in self.payload:
            self.payload["tasks"] = []
        for task in tasks:
            self.payload["tasks"].append(task.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class DAGTask(object):
    # https://argoproj.github.io/argo-workflows/fields/#dagtask

    # TODO: Handle withItems, withParam and withSequence

    def __init__(self, name):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name

    def arguments(self, arguments):
        self.payload["arguments"] = arguments.to_json()
        return self

    def dependencies(self, dependencies):
        self.payload["dependencies"] = dependencies
        return self

    def template(self, template):
        # Template reference
        self.payload["template"] = template
        return self

    def inline(self, template):
        # We could have inlined the template here but
        # https://github.com/argoproj/argo-workflows/issues/7432 prevents us for now.
        self.payload["inline"] = template.to_json()
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Arguments(object):
    # https://argoproj.github.io/argo-workflows/fields/#arguments

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def parameters(self, parameters):
        if "parameters" not in self.payload:
            self.payload["parameters"] = []
        for parameter in parameters:
            self.payload["parameters"].append(parameter.to_json())
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)
