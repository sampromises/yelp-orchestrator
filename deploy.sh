#!/bin/bash
python -m pytest tests/unit \
&& cdk synth && cdk deploy --require-approval never
