#!/usr/bin/env python3

from aws_cdk import core

from yelp_orchestrator.yelp_orchestrator_stack import YelpOrchestratorStack


app = core.App()
YelpOrchestratorStack(app, "yelp-orchestrator")

app.synth()
