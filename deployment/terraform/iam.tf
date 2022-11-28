#
# GitHub Actions IAM resources
#
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Federated"
      identifiers = [data.aws_iam_openid_connect_provider.github.arn]
    }

    actions = ["sts:AssumeRoleWithWebIdentity"]

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"

      values = [
        "sts.amazonaws.com"
      ]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"

      values = [
        for ref
        in var.refs_that_can_assume_github_actions_role
        : "repo:${var.repo_name}:ref:${ref}"
      ]
    }
  }
}

resource "aws_iam_role" "github_actions_role" {
  name_prefix        = "GitHubActions${local.short}Role-"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume_role.json

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "github_actions_role_policy" {
  role       = aws_iam_role.github_actions_role.name
  policy_arn = var.aws_administrator_access_policy_arn
}

#
# Batch IAM resources
#
data "aws_iam_policy_document" "batch_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["batch.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "batch_service_role" {
  name_prefix        = "batch${local.short}ServiceRole-"
  assume_role_policy = data.aws_iam_policy_document.batch_assume_role.json

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "batch_service_role_policy" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = var.aws_batch_service_role_policy_arn
}


#
# Spot Fleet IAM resources
#
data "aws_iam_policy_document" "spot_fleet_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["spotfleet.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "spot_fleet_service_role" {
  name_prefix        = "fleet${local.short}ServiceRole-"
  assume_role_policy = data.aws_iam_policy_document.spot_fleet_assume_role.json

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "spot_fleet_service_role_policy" {
  role       = aws_iam_role.spot_fleet_service_role.name
  policy_arn = var.aws_spot_fleet_service_role_policy_arn
}

#
# EC2 IAM resources
#
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com","ecs-tasks.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ecs_instance_role" {
  name_prefix        = "ecs${local.short}InstanceRole-"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

#Attaching policy to view secrets for ecs task

data "aws_iam_policy_document" "secrets_service_role_policy" {
  statement {
    effect = "Allow"
    
    actions = [
      "secretsmanager:GetSecretValue"
    ]

    resources = [
      "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:${var.project}/${var.environment}/*"
    ]
  }
}

resource "aws_iam_role_policy" "secrets_ecs_service_role_policy" {
  name_prefix = "secretsServiceRolePolicy"
  role        = aws_iam_role.ecs_instance_role.name
  policy      = data.aws_iam_policy_document.secrets_service_role_policy.json
}

resource "aws_iam_role_policy_attachment" "ec2_service_role_policy" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = var.aws_ec2_service_role_policy_arn
}

resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = aws_iam_role.ecs_instance_role.name
  role = aws_iam_role.ecs_instance_role.name

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

#
# Step Functions IAM resources
#
data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "step_functions_service_role_policy" {
  statement {
    effect = "Allow"

    actions = [
      "batch:SubmitJob",
      "batch:DescribeJobs",
      "batch:TerminateJobs"
    ]

    # Despite the "*" wildcard, only allow these actions for Batch jobs that were
    # started by Step Functions.
    # See: https://github.com/awsdocs/aws-step-functions-developer-guide/blob/master/doc_source/batch-iam.md
    resources = ["*"]
  }

  statement {
    effect = "Allow"

    actions = [
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
    ]

    resources = [
      "arn:aws:events:${var.region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForBatchJobsRule",
    ]
  }
}

resource "aws_iam_role" "step_functions_service_role" {
  name_prefix        = "sfn${local.short}ServiceRole"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "step_functions_service_role_policy" {
  name_prefix = "sfnServiceRolePolicy"
  role        = aws_iam_role.step_functions_service_role.name
  policy      = data.aws_iam_policy_document.step_functions_service_role_policy.json
}

## Cloudwatch Evant Bus IAM ###

data "aws_iam_policy_document" "cw_eventbus_assume_role" {

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventbus" {
  name_prefix        = "EventBus${local.short}Role-"

  assume_role_policy    = data.aws_iam_policy_document.cw_eventbus_assume_role.json

  tags = {
     Project = var.project
     Environment = var.environment
  }
}

data "aws_iam_policy_document" "sfn" {

  statement {
    sid       = "StepFunctionAccess"
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.default.arn] 
  }
}

resource "aws_iam_policy" "sfn" {

  name   = "${local.short}-sfn"
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_iam_policy_attachment" "sfn" {

  name        = "EB${local.short}Role-"
  roles      = [aws_iam_role.eventbus.name]
  policy_arn = aws_iam_policy.sfn.arn
}
