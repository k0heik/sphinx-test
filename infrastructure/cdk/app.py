#!/usr/bin/env python3
import aws_cdk as cdk

from cdk.codebuild_stack import CodeBuildStack


app = cdk.App()
target_env = app.node.try_get_context('target_env')
debug_name = app.node.try_get_context('debug_name')
if debug_name is None:
    debug_name = ""

valid_target_envs = ["PreProduction", "Staging", "Production"]
if not (target_env in valid_target_envs):
    raise ValueError(f"target_env must specified any in {valid_target_envs}.")

codebuild_stack_name = f"{debug_name}{target_env}BidOptimisationML-CDKCodeBuildStack"

CodeBuildStack(app, codebuild_stack_name)

app.synth()
