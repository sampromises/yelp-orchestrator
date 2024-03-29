#!/usr/bin/env python3
import os
import subprocess
from os import path

from aws_cdk import (
    aws_apigateway,
    aws_cloudwatch,
    aws_cloudwatch_actions,
    aws_dynamodb,
    aws_events,
    aws_events_targets,
    aws_lambda,
    aws_lambda_event_sources,
    aws_s3,
    aws_s3_notifications,
    aws_sns,
    aws_sns_subscriptions,
    core,
)

FETCH_BATCH_SIZE = os.environ["FETCH_BATCH_SIZE"]
URL_TABLE_TTL = os.environ["URL_TABLE_TTL"]
YELP_TABLE_TTL = os.environ["YELP_TABLE_TTL"]
ALARM_TOPIC_EMAIL = os.environ["ALARM_TOPIC_EMAIL"]
STACK_NAME = "YelpOrchestrator"
API_NAME = "YelpOrchestratorAPI"
URL_TABLE_NAME = "UrlTable"
YELP_TABLE_NAME = "YelpTable"
CONFIG_TABLE_NAME = "ConfigTable"
PAGE_BUCKET_NAME = "YelpOrchestratorPageBucket"


class YelpOrchestratorStack(core.Stack):
    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.create_alarm_topic()

        self.create_page_bucket()
        self.create_url_table()
        self.create_yelp_table()
        self.create_config_table()

        self._lambdas = (
            self.create_url_requester(),
            self.create_page_fetcher(),
            self.create_yelp_parser(),
            self.create_apig_handler(),
            self.create_yelp_cleaner(),
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
        url_table = aws_dynamodb.Table(
            self,
            URL_TABLE_NAME,
            table_name=URL_TABLE_NAME,
            partition_key=aws_dynamodb.Attribute(
                name="UserId", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(name="SortKey", type=aws_dynamodb.AttributeType.STRING),
            time_to_live_attribute="TimeToLive",
        )
        url_table.add_global_secondary_index(
            partition_key=aws_dynamodb.Attribute(
                name="PageUrl", type=aws_dynamodb.AttributeType.STRING
            ),
            index_name="PageUrl",
        )
        self.url_table = url_table

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
            stream=aws_dynamodb.StreamViewType.NEW_IMAGE,
        )

    def create_url_requester(self):
        url_requester = self.create_lambda_with_error_alarm("url_requester")
        url_requester.add_event_source(
            aws_lambda_event_sources.DynamoEventSource(
                self.yelp_table,
                starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=5,
                bisect_batch_on_error=True,
                retry_attempts=0,
            )
        )
        url_requester.add_event_source(
            aws_lambda_event_sources.DynamoEventSource(
                self.config_table,
                starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
                batch_size=5,
                bisect_batch_on_error=True,
                retry_attempts=0,
            )
        )
        rule = aws_events.Rule(
            self,
            "UrlRequesterRule",
            schedule=aws_events.Schedule.cron(
                minute="*/5", hour="*", month="*", week_day="*", year="*"
            ),
        )
        rule.add_target(aws_events_targets.LambdaFunction(url_requester))
        self.url_requester = url_requester
        return self.url_requester

    def create_page_fetcher(self):
        page_fetcher = self.create_lambda_with_error_alarm("page_fetcher")
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

    def create_apig_handler(self):
        apig_handler = self.create_lambda_with_error_alarm("apig_handler")
        self.apig_handler = apig_handler
        return self.apig_handler

    def create_yelp_parser(self):
        yelp_parser = self.create_lambda_with_error_alarm("yelp_parser")
        self.page_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.LambdaDestination(yelp_parser),
        )
        self.yelp_parser = yelp_parser
        return self.yelp_parser

    def create_yelp_cleaner(self):
        yelp_cleaner = self.create_lambda_with_error_alarm("yelp_cleaner", memory_size=256)
        rule = aws_events.Rule(
            self,
            "YelpCleanerRule",
            schedule=aws_events.Schedule.cron(
                minute=f"*/15",
                hour="*",
                month="*",
                week_day="*",
                year="*",
            ),
        )
        rule.add_target(aws_events_targets.LambdaFunction(yelp_cleaner))
        self.yelp_cleaner = yelp_cleaner
        return self.yelp_cleaner

    def create_lambda_with_error_alarm(self, lambda_name, memory_size=128):
        _lambda = aws_lambda.Function(
            self,
            self.snake_to_pascal_case(lambda_name),
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            handler=f"yelp.{lambda_name}.handle",
            code=aws_lambda.Code.asset("./src"),
            timeout=core.Duration.seconds(300),
            layers=self.create_dependencies_layer(lambda_name),
            memory_size=memory_size,
        )
        self.create_error_alarm(
            alarm_name=f"{self.snake_to_pascal_case(lambda_name)}ErrorAlarm",
            error_metric=_lambda.metric_errors(period=core.Duration.minutes(5), statistic="sum"),
        )
        return _lambda

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
        apig = aws_apigateway.LambdaRestApi(
            self, API_NAME, handler=self.apig_handler, proxy=False, deploy=True
        )

        users = apig.root.add_resource("users")
        users.add_method("GET")

        user = apig.root.add_resource("{userId}")
        user.add_method("GET")
        user.add_method("POST")
        user.add_method("DELETE")

        self.apig = apig

    def add_permissions(self):
        # Add permissions
        self.config_table.grant_read_write_data(self.apig_handler)
        self.config_table.grant_read_data(self.url_requester)
        self.config_table.grant_read_data(self.yelp_cleaner)
        self.yelp_table.grant_read_write_data(self.apig_handler)
        self.yelp_table.grant_read_data(self.url_requester)
        self.yelp_table.grant_read_write_data(self.yelp_parser)
        self.yelp_table.grant_read_write_data(self.yelp_cleaner)
        self.url_table.grant_read_write_data(self.apig_handler)
        self.url_table.grant_read_write_data(self.url_requester)
        self.url_table.grant_read_write_data(self.page_fetcher)
        self.url_table.grant_read_write_data(self.yelp_cleaner)
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
            self.text_widget("YelpCleaner", "#"),
            *self.get_generic_lambda_graphs(self.yelp_cleaner),
            self.get_yelp_cleaner_graph(),
        )
        self.dashboard = dashboard

    def create_alarm_topic(self):
        topic_name = f"{STACK_NAME}ErrorTopic"
        topic = aws_sns.Topic(self, id=topic_name, topic_name=topic_name)
        topic.add_subscription(aws_sns_subscriptions.EmailSubscription(ALARM_TOPIC_EMAIL))
        self.alarm_topic = topic

    def create_error_alarm(self, alarm_name, error_metric):
        alarm = aws_cloudwatch.Alarm(
            self,
            alarm_name,
            alarm_name=alarm_name,
            metric=error_metric,
            evaluation_periods=1,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            datapoints_to_alarm=1,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        alarm.add_alarm_action(aws_cloudwatch_actions.SnsAction(self.alarm_topic))
        return alarm

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
    def get_yelp_cleaner_graph():
        return YelpOrchestratorStack.graph_widget(
            "YelpCleanerDeletions",
            *[
                aws_cloudwatch.Metric(
                    namespace="YelpOrchestrator",
                    metric_name=metric_name,
                    statistic="Sum",
                    period=core.Duration.minutes(5),
                )
                for metric_name in ("UrlTableRecordsDeleted", "YelpTableRecordsDeleted")
            ],
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
