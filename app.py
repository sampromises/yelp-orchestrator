#!/usr/bin/env python3
import os
import subprocess
from os import path

from aws_cdk import (
    aws_apigateway,
    aws_cloudwatch,
    aws_dynamodb,
    aws_events,
    aws_events_targets,
    aws_lambda,
    aws_lambda_event_sources,
    aws_s3,
    aws_s3_notifications,
    core,
)

FETCH_BATCH_SIZE = os.environ["FETCH_BATCH_SIZE"]
URL_TABLE_TTL = os.environ["URL_TABLE_TTL"]
YELP_TABLE_TTL = os.environ["YELP_TABLE_TTL"]

STACK_NAME = "YelpOrchestrator"
API_NAME = "YelpOrchestratorAPI"
URL_TABLE_NAME = "UrlTable"
YELP_TABLE_NAME = "YelpTable"
CONFIG_TABLE_NAME = "ConfigTable"
PAGE_BUCKET_NAME = "YelpOrchestratorPageBucket"


class YelpOrchestratorStack(core.Stack):
    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.create_page_bucket()
        self.create_url_table()
        self.create_yelp_table()
        self.create_config_table()

        self._lambdas = (
            self.create_url_requester(),
            self.create_page_fetcher(),
            self.create_yelp_parser(),
            self.create_apig_handler(),
        )

        self.create_apigateway()

        self.add_permissions()
        self.add_env_vars()

        self.create_dashboard()

    def create_page_bucket(self):
        self.page_bucket = aws_s3.Bucket(
            self,
            PAGE_BUCKET_NAME,
            lifecycle_rules=[aws_s3.LifecycleRule(expiration=core.Duration.days(1))],
        )

    def create_url_table(self):
        self.url_table = aws_dynamodb.Table(
            self,
            URL_TABLE_NAME,
            table_name=URL_TABLE_NAME,
            partition_key=aws_dynamodb.Attribute(
                name="Url", type=aws_dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="TimeToLive",
        )

    def create_yelp_table(self):
        yelp_table = aws_dynamodb.Table(
            self,
            YELP_TABLE_NAME,
            table_name=YELP_TABLE_NAME,
            partition_key=aws_dynamodb.Attribute(
                name="UserId", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(name="SortKey", type=aws_dynamodb.AttributeType.STRING),
            stream=aws_dynamodb.StreamViewType.NEW_IMAGE,
            time_to_live_attribute="TimeToLive",
        )
        yelp_table.add_global_secondary_index(
            partition_key=aws_dynamodb.Attribute(
                name="ReviewId", type=aws_dynamodb.AttributeType.STRING
            ),
            index_name="ReviewId",
        )
        self.yelp_table = yelp_table

    def create_config_table(self):
        self.config_table = aws_dynamodb.Table(
            self,
            CONFIG_TABLE_NAME,
            table_name=CONFIG_TABLE_NAME,
            partition_key=aws_dynamodb.Attribute(
                name="UserId", type=aws_dynamodb.AttributeType.STRING
            ),
        )

    def create_url_requester(self):
        url_requester = self.create_lambda("url_requester")
        url_requester.add_event_source(
            aws_lambda_event_sources.DynamoEventSource(
                self.yelp_table,
                starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=5,
                bisect_batch_on_error=True,
            )
        )
        self.url_requester = url_requester
        return self.url_requester

    def create_page_fetcher(self):
        page_fetcher = self.create_lambda("page_fetcher")
        rule = aws_events.Rule(
            self,
            "PageFetcherRule",
            schedule=aws_events.Schedule.cron(
                minute="*/5", hour="*", month="*", week_day="*", year="*"
            ),
        )
        rule.add_target(aws_events_targets.LambdaFunction(page_fetcher))
        self.page_fetcher = page_fetcher
        return self.page_fetcher

    def create_yelp_parser(self):
        yelp_parser = self.create_lambda("yelp_parser")
        self.page_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.LambdaDestination(yelp_parser),
        )
        self.yelp_parser = yelp_parser
        return self.yelp_parser

    def create_apig_handler(self):
        apig_handler = self.create_lambda("apig_handler")
        self.apig_handler = apig_handler
        return self.apig_handler

    def create_lambda(self, lambda_name):
        return aws_lambda.Function(
            self,
            self.snake_to_pascal_case(lambda_name),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            handler=f"yelp.{lambda_name}.handle",
            code=aws_lambda.Code.asset("./src"),
            timeout=core.Duration.seconds(300),
            layers=self.create_dependencies_layer(lambda_name),
        )

    # Taken from: https://stackoverflow.com/a/61248003
    def create_dependencies_layer(self, function_name) -> aws_lambda.LayerVersion:
        requirements_file = "lambda_dependencies/" + function_name + ".txt"
        output_dir = ".lambda_dependencies/" + function_name

        # Install requirements for layer in the output_dir
        if path.exists(requirements_file):
            # Note: Pip will create the output dir if it does not exist
            subprocess.check_call(
                f"pip install -r {requirements_file} -t {output_dir}/python".split()
            )
            return [
                aws_lambda.LayerVersion(
                    self,
                    STACK_NAME + "-" + function_name + "-dependencies",
                    code=aws_lambda.Code.from_asset(output_dir),
                )
            ]

        return []

    def create_apigateway(self):
        self.apig = aws_apigateway.LambdaRestApi(
            self, API_NAME, handler=self.apig_handler, proxy=True, deploy=True
        )

    def add_permissions(self):
        # Add permissions
        self.yelp_table.grant_read_data(self.apig_handler)
        self.yelp_table.grant_read_data(self.url_requester)
        self.yelp_table.grant_read_write_data(self.yelp_parser)
        self.url_table.grant_read_write_data(self.url_requester)
        self.url_table.grant_read_write_data(self.page_fetcher)
        self.page_bucket.grant_read_write(self.yelp_parser)
        self.page_bucket.grant_read_write(self.page_fetcher)

    def add_env_vars(self):
        env_vars_to_add = {
            "YELP_TABLE_NAME": self.yelp_table.table_name,
            "URL_TABLE_NAME": self.url_table.table_name,
            "CONFIG_TABLE_NAME": self.config_table.table_name,
            "PAGE_BUCKET_NAME": self.page_bucket.bucket_name,
            "FETCH_BATCH_SIZE": FETCH_BATCH_SIZE,
            "URL_TABLE_TTL": URL_TABLE_TTL,
            "YELP_TABLE_TTL": YELP_TABLE_TTL,
        }
        for _lambda in self._lambdas:
            for k, v in env_vars_to_add.items():
                _lambda.add_environment(k, v)

    def create_dashboard(self):
        dashboard = aws_cloudwatch.Dashboard(self, "YelpOrchestratorDashboard", start="-P1W")
        dashboard.add_widgets(
            self.text_widget("PageBucket", "#"),
            *self.get_s3_graphs(self.page_bucket),
            self.text_widget("APIGateway", "#"),
            *self.get_generic_apig_graphs(self.apig),
            self.text_widget("ApiGatewayHandler", "#"),
            *self.get_generic_lambda_graphs(self.apig_handler),
            self.text_widget("YelpParser", "#"),
            *self.get_generic_lambda_graphs(self.yelp_parser),
            self.text_widget("UrlRequester", "#"),
            *self.get_generic_lambda_graphs(self.url_requester),
            self.text_widget("PageFetcher", "#"),
            *self.get_generic_lambda_graphs(self.page_fetcher),
        )
        self.dashboard = dashboard

    @staticmethod
    def get_s3_graphs(bucket):
        return (
            YelpOrchestratorStack.graph_widget(
                "ObjectCount",
                aws_cloudwatch.Metric(
                    namespace="AWS/S3",
                    metric_name="NumberOfObjects",
                    dimensions={
                        "StorageType": "AllStorageTypes",
                        "BucketName": bucket.bucket_name,
                    },
                    statistic="Sum",
                    period=core.Duration.minutes(5),
                ),
            ),
        )

    @staticmethod
    def get_generic_apig_graphs(apig):
        return (
            YelpOrchestratorStack.graph_widget("Count", apig.metric_count()),
            YelpOrchestratorStack.graph_widget(
                "Errors", apig.metric_client_error(), apig.metric_server_error()
            ),
        )

    @staticmethod
    def get_generic_lambda_graphs(_lambda):
        return (
            YelpOrchestratorStack.graph_widget("Invocations", _lambda.metric_invocations()),
            YelpOrchestratorStack.graph_widget("Duration", _lambda.metric_duration()),
            YelpOrchestratorStack.graph_widget("Errors", _lambda.metric_errors()),
        )

    @staticmethod
    def text_widget(text, size="###"):
        return aws_cloudwatch.TextWidget(markdown=f"{size} {text}", height=1, width=24)

    @staticmethod
    def graph_widget(title, *metrics):
        return aws_cloudwatch.GraphWidget(title=title, left=list(metrics), height=6, width=8)

    @staticmethod
    def snake_to_pascal_case(name):
        return "".join(map(lambda x: x.capitalize(), name.split("_")))


app = core.App()
YelpOrchestratorStack(app, STACK_NAME)
app.synth()
