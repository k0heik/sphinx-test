from aws_cdk import (
    Stack,
    aws_codebuild as codebuild,
    aws_logs as logs,
    aws_iam as iam,
    Duration,
)
from constructs import Construct


SUBSYSTEM_SUFFIXES = [
    "Common-SlackNotification",
    "Common-CloudFormation",
    "-CPC",
    "-CVR",
    "-SPA",
    "-Main",
    "-RecordToBq",
    "-TrainPipeline",
]

class CodeBuildStack(Stack):

    GITHUB_LOCATION = "https://github.com/negocia-inc/bid_optimisation_ml.git"

    branch_dict = {
        "PreProduction": "develop",
        "Staging": "staging",
        "Production": "main",
    }
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        target_env = scope.node.try_get_context('target_env')
        debug_name = scope.node.try_get_context('debug_name')
        debug_branch = scope.node.try_get_context('debug_branch')
        if debug_name is None:
            debug_name = ""

        print(self.account)
        aws_account = self.account
        ecr_repo = f'{aws_account}.dkr.ecr.ap-northeast-1.amazonaws.com/negocia/optimise-bidding-ml'
        role_arn = f"arn:aws:iam::{aws_account}:role/OptimiseBiddingMLCodeBuild"
        branch_or_ref = debug_branch if debug_branch else self.branch_dict[target_env]

        print(f"branch_or_ref: {branch_or_ref}")

        github_source = codebuild.Source.git_hub(
            owner="negocia-inc",
            repo="bid_optimisation_ml",
            branch_or_ref=branch_or_ref,
            webhook=False,  # optional, default: true if `webhookFilters` were provided, false otherwise
            webhook_triggers_batch_build=False,  # optional, default is false
            clone_depth=1
        )
        role = iam.Role.from_role_arn(
            self, "Role",
            role_arn=role_arn,
            add_grants_to_resources=True,
            mutable=False
        )

        for subsystem_suffix in SUBSYSTEM_SUFFIXES:
            codebuild_project_name = f'{debug_name}{target_env}OptimiseBiddingML{subsystem_suffix}'
            print(f"codebuild_project_name: {codebuild_project_name}")
            build_project = codebuild.Project(
                self, codebuild_project_name,
                project_name=codebuild_project_name,
                source=github_source,
                build_spec=codebuild.BuildSpec.from_source_filename(
                    f"infrastructure/OptimiseBiddingML{subsystem_suffix}/buildspec.yml"),
                environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_5_0,
                    compute_type=codebuild.ComputeType.SMALL,
                    environment_variables={
                        "TARGET_ENV": codebuild.BuildEnvironmentVariable(
                            value=target_env),
                        "AWS_ACCOUNT": codebuild.BuildEnvironmentVariable(
                            value=aws_account),
                        "ECR_REPO": codebuild.BuildEnvironmentVariable(
                            value=ecr_repo),
                    },
                    privileged=True,
                ),
                queued_timeout=Duration.minutes(480),
                role=role,
                timeout=Duration.minutes(60)
            )
