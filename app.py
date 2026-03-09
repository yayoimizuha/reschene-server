#!/usr/bin/env python3
"""CDK entrypoint for Reschene.

All resources are in a single stack to avoid circular dependency issues
between S3 event notifications, Lambda functions, and ECS task definitions.
"""

import os

import aws_cdk as cdk
from dotenv import load_dotenv

from stacks.reschene_stack import RescheneStack

load_dotenv()

app = cdk.App()

RescheneStack(
    app,
    "Reschene",
    google_client_id=os.environ["GOOGLE_CLIENT_ID"],
    google_client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
)

app.synth()
