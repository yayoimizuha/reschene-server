"""Reschene - Single stack containing all resources.

Combines storage, auth, analytics, API, processing, and reconstruction
into one stack to avoid circular dependency issues with S3 event notifications.
"""

import aws_cdk as cdk
from aws_cdk import aws_apigatewayv2 as apigwv2
from aws_cdk import aws_apigatewayv2_authorizers as apigwv2_auth
from aws_cdk import aws_apigatewayv2_integrations as apigwv2_int
from aws_cdk import aws_athena as athena
from aws_cdk import aws_autoscaling as autoscaling
from aws_cdk import aws_cognito as cognito
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from constructs import Construct


class RescheneStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        google_client_id: str,
        google_client_secret: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ================================================================
        # 1. Auth (Cognito)
        # ================================================================
        user_pool = cognito.UserPool(
            self,
            "UserPool",
            user_pool_name="reschene-user-pool",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=False,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # Cognito Domain (required for Hosted UI / OAuth flows)
        user_pool_domain = user_pool.add_domain(
            "CognitoDomain",
            cognito_domain=cognito.CognitoDomainOptions(
                domain_prefix="reschene",
            ),
        )

        # Google Identity Provider
        google_idp = cognito.UserPoolIdentityProviderGoogle(
            self,
            "GoogleIdP",
            user_pool=user_pool,
            client_id=google_client_id,
            client_secret_value=cdk.SecretValue.unsafe_plain_text(google_client_secret),
            scopes=["openid", "email", "profile"],
            attribute_mapping=cognito.AttributeMapping(
                email=cognito.ProviderAttribute.GOOGLE_EMAIL,
                fullname=cognito.ProviderAttribute.GOOGLE_NAME,
                profile_picture=cognito.ProviderAttribute.GOOGLE_PICTURE,
            ),
        )

        user_pool_client = user_pool.add_client(
            "AppClient",
            user_pool_client_name="reschene-app-client",
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
            ),
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(
                    authorization_code_grant=True,
                ),
                scopes=[
                    cognito.OAuthScope.OPENID,
                    cognito.OAuthScope.EMAIL,
                    cognito.OAuthScope.PROFILE,
                ],
                callback_urls=["http://localhost:3000/callback"],
                logout_urls=["http://localhost:3000/"],
            ),
            supported_identity_providers=[
                cognito.UserPoolClientIdentityProvider.GOOGLE,
                cognito.UserPoolClientIdentityProvider.COGNITO,
            ],
            id_token_validity=cdk.Duration.hours(1),
            access_token_validity=cdk.Duration.hours(1),
            refresh_token_validity=cdk.Duration.days(30),
        )
        # Ensure Google IdP is created before the client references it
        user_pool_client.node.add_dependency(google_idp)

        # ================================================================
        # 2. Storage (S3 Buckets)
        # ================================================================
        image_bucket = s3.Bucket(
            self,
            "ImageBucket",
            bucket_name="reschene-userimage",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            cors=[
                s3.CorsRule(
                    allowed_methods=[s3.HttpMethods.PUT],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3600,
                )
            ],
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        metadata_bucket = s3.Bucket(
            self,
            "MetadataBucket",
            bucket_name="reschene-metadata",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            bucket_name="reschene-athena-results",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=cdk.Duration.days(7)),
            ],
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        output_3d_bucket = s3.Bucket(
            self,
            "Output3DBucket",
            bucket_name="reschene-3d-output",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        thumbnail_bucket = s3.Bucket(
            self,
            "ThumbnailBucket",
            bucket_name="reschene-thumbnails",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ================================================================
        # 3. Analytics (Athena + Glue)
        # ================================================================
        database_name = "reschene"
        raw_table_name = "image_metadata_raw"
        compacted_table_name = "image_metadata_compacted"
        view_name = "image_metadata"
        workgroup_name = "reschene-workgroup"

        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description="Reschene image metadata database",
            ),
        )

        # Column definitions shared by both tables
        metadata_columns = [
            glue.CfnTable.ColumnProperty(name="user_id", type="string"),
            glue.CfnTable.ColumnProperty(name="s3_key", type="string"),
            glue.CfnTable.ColumnProperty(name="upload_id", type="string"),
            glue.CfnTable.ColumnProperty(name="original_filename", type="string"),
            glue.CfnTable.ColumnProperty(name="file_size", type="bigint"),
            glue.CfnTable.ColumnProperty(name="uploaded_at", type="string"),
            glue.CfnTable.ColumnProperty(name="camera_make", type="string"),
            glue.CfnTable.ColumnProperty(name="camera_model", type="string"),
            glue.CfnTable.ColumnProperty(name="datetime_original", type="string"),
            glue.CfnTable.ColumnProperty(name="gps_latitude", type="double"),
            glue.CfnTable.ColumnProperty(name="gps_longitude", type="double"),
            glue.CfnTable.ColumnProperty(name="gps_altitude", type="double"),
        ]

        # Raw table: per-image JSON files written by metadata_extraction Lambda
        raw_table = glue.CfnTable(
            self,
            "GlueTableRaw",
            catalog_id=self.account,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=raw_table_name,
                description="Raw per-image metadata JSON files (pre-compaction)",
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "json"},
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=metadata_columns,
                    location=f"s3://{metadata_bucket.bucket_name}/raw/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                    ),
                ),
            ),
        )
        raw_table.add_dependency(glue_database)

        # Compacted table: Parquet file(s) produced by compaction Lambda
        compacted_table = glue.CfnTable(
            self,
            "GlueTableCompacted",
            catalog_id=self.account,
            database_name=database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=compacted_table_name,
                description="Compacted image metadata in Parquet format",
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "parquet"},
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=metadata_columns,
                    location=f"s3://{metadata_bucket.bucket_name}/compacted/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    ),
                ),
            ),
        )
        compacted_table.add_dependency(glue_database)

        athena_workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkgroup",
            name=workgroup_name,
            description="Reschene query workgroup",
            state="ENABLED",
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3",
                ),
            ),
        )

        # ================================================================
        # 4. Reconstruction (VPC + ECS + GPU)
        # ================================================================
        vpc = ec2.Vpc(
            self,
            "ReconstructionVpc",
            vpc_name="reschene-reconstruction-vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
            ],
        )

        # S3 Gateway VPC Endpoint - allows ECS tasks to reach S3 without NAT or public IP
        vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        ecs_security_group = ec2.SecurityGroup(
            self,
            "EcsSecurityGroup",
            vpc=vpc,
            security_group_name="reschene-ecs-sg",
            description="Security group for Reschene ECS reconstruction tasks",
            allow_all_outbound=True,
        )

        cluster = ecs.Cluster(
            self,
            "ReconstructionCluster",
            cluster_name="reschene-reconstruction",
            vpc=vpc,
        )

        # GPU AMI (ECS-Optimized GPU)
        gpu_ami = ecs.EcsOptimizedImage.amazon_linux2(
            hardware_type=ecs.AmiHardwareType.GPU,
        )

        # Launch Template (required for new AWS accounts that don't support Launch Configurations)
        launch_template = ec2.LaunchTemplate(
            self,
            "GpuLaunchTemplate",
            launch_template_name="reschene-gpu-launch-template",
            instance_type=ec2.InstanceType("g4dn.xlarge"),
            machine_image=gpu_ami,
            security_group=ecs_security_group,
            user_data=ec2.UserData.for_linux(),
            role=iam.Role(
                self,
                "GpuAsgInstanceRole",
                assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEC2ContainerServiceforEC2Role"),
                    iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
                ],
            ),
        )

        asg = autoscaling.AutoScalingGroup(
            self,
            "GpuAsg",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            launch_template=launch_template,
            min_capacity=0,
            max_capacity=2,
            desired_capacity=0,
            new_instances_protected_from_scale_in=False,
        )

        capacity_provider = ecs.AsgCapacityProvider(
            self,
            "GpuCapacityProvider",
            auto_scaling_group=asg,
            enable_managed_scaling=True,
            enable_managed_termination_protection=False,
        )
        cluster.add_asg_capacity_provider(capacity_provider)

        # ECS Task Definition
        task_definition = ecs.Ec2TaskDefinition(
            self,
            "ReconstructionTaskDef",
            network_mode=ecs.NetworkMode.AWS_VPC,
        )

        container_image = ecs.ContainerImage.from_asset("containers/reconstruction")

        task_definition.add_container(
            "reconstruction",
            image=container_image,
            memory_limit_mib=7680,
            gpu_count=1,
            logging=ecs.LogDrivers.aws_logs(stream_prefix="reconstruction"),
            environment={
                "IMAGE_BUCKET": image_bucket.bucket_name,
                "OUTPUT_BUCKET": output_3d_bucket.bucket_name,
            },
        )

        # Task role permissions
        image_bucket.grant_read(task_definition.task_role)
        output_3d_bucket.grant_read_write(task_definition.task_role)

        # SNS Topic for notifications
        notification_topic = sns.Topic(
            self,
            "ReconstructionNotificationTopic",
            topic_name="reschene-reconstruction-notifications",
        )

        # Reconstruction Judge Lambda
        judge_function = lambda_.Function(
            self,
            "ReconstructionJudgeFunction",
            function_name="reschene-reconstruction-judge",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/reconstruction_judge"),
            memory_size=256,
            timeout=cdk.Duration.seconds(60),
            environment={
                "RECONSTRUCTION_RADIUS_KM": "1.0",
                "RECONSTRUCTION_THRESHOLD": "50",
                "ECS_CLUSTER_ARN": cluster.cluster_arn,
                "ECS_TASK_DEFINITION_ARN": task_definition.task_definition_arn,
                "ECS_SUBNET_IDS": ",".join([s.subnet_id for s in vpc.public_subnets]),
                "ECS_SECURITY_GROUP_IDS": ecs_security_group.security_group_id,
                "ECS_CAPACITY_PROVIDER": capacity_provider.capacity_provider_name,
                "OUTPUT_BUCKET": output_3d_bucket.bucket_name,
                "IMAGE_BUCKET": image_bucket.bucket_name,
                "ATHENA_WORKGROUP": workgroup_name,
                "GLUE_DATABASE": database_name,
                "GLUE_TABLE_RAW": raw_table_name,
                "GLUE_TABLE_COMPACTED": compacted_table_name,
            },
        )

        # Judge Lambda permissions
        output_3d_bucket.grant_read_write(judge_function)
        judge_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask", "ecs:DescribeTasks"],
                resources=["*"],
            )
        )
        pass_role_resources = [task_definition.task_role.role_arn]
        if task_definition.execution_role:
            pass_role_resources.append(task_definition.execution_role.role_arn)
        judge_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=pass_role_resources,
            )
        )
        judge_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=["*"],
            )
        )
        judge_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                resources=["*"],
            )
        )
        athena_results_bucket.grant_read_write(judge_function)
        judge_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
                resources=[
                    metadata_bucket.bucket_arn,
                    f"{metadata_bucket.bucket_arn}/*",
                ],
            )
        )

        # EventBridge: ECS Task State Change → SNS
        events.Rule(
            self,
            "EcsTaskStateChangeRule",
            rule_name="reschene-ecs-task-state-change",
            event_pattern=events.EventPattern(
                source=["aws.ecs"],
                detail_type=["ECS Task State Change"],
                detail={
                    "clusterArn": [cluster.cluster_arn],
                    "lastStatus": ["RUNNING", "STOPPED"],
                },
            ),
            targets=[events_targets.SnsTopic(notification_topic)],
        )

        # ================================================================
        # 5. Processing (Metadata extraction + Cleanup Lambdas)
        # ================================================================
        metadata_function = lambda_.Function(
            self,
            "MetadataExtractionFunction",
            function_name="reschene-metadata-extraction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                "lambdas/metadata_extraction",
                bundling=cdk.BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            memory_size=512,
            timeout=cdk.Duration.seconds(30),
            environment={
                "IMAGE_BUCKET": image_bucket.bucket_name,
                "METADATA_BUCKET": metadata_bucket.bucket_name,
                "THUMBNAIL_BUCKET": thumbnail_bucket.bucket_name,
                "RECONSTRUCTION_JUDGE_FUNCTION_ARN": judge_function.function_arn,
            },
        )

        image_bucket.grant_read(metadata_function)
        metadata_bucket.grant_read_write(metadata_function)
        thumbnail_bucket.grant_write(metadata_function)
        judge_function.grant_invoke(metadata_function)

        # S3 trigger: ObjectCreated → metadata extraction
        image_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(metadata_function),
        )

        # Cleanup Lambda
        cleanup_function = lambda_.Function(
            self,
            "CleanupFunction",
            function_name="reschene-cleanup",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/cleanup"),
            memory_size=256,
            timeout=cdk.Duration.seconds(30),
            environment={
                "METADATA_BUCKET": metadata_bucket.bucket_name,
                "THUMBNAIL_BUCKET": thumbnail_bucket.bucket_name,
            },
        )

        metadata_bucket.grant_read_write(cleanup_function)
        thumbnail_bucket.grant_delete(cleanup_function)

        # S3 trigger: ObjectRemoved → cleanup
        image_bucket.add_event_notification(
            s3.EventType.OBJECT_REMOVED,
            s3n.LambdaDestination(cleanup_function),
        )

        # Compaction Lambda (merges raw per-image JSON → Parquet)
        compaction_function = lambda_.Function(
            self,
            "CompactionFunction",
            function_name="reschene-compaction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                "lambdas/compaction",
                bundling=cdk.BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            memory_size=1024,
            timeout=cdk.Duration.minutes(5),
            environment={
                "METADATA_BUCKET": metadata_bucket.bucket_name,
            },
        )

        metadata_bucket.grant_read_write(compaction_function)

        # EventBridge Rule: run compaction daily at 03:00 UTC
        compaction_schedule = events.Rule(
            self,
            "CompactionSchedule",
            rule_name="reschene-compaction-daily",
            schedule=events.Schedule.cron(minute="0", hour="3"),
            targets=[events_targets.LambdaFunction(compaction_function)],
        )

        # ================================================================
        # 6. API Gateway
        # ================================================================
        authorizer = apigwv2_auth.HttpJwtAuthorizer(
            "CognitoAuthorizer",
            jwt_issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{user_pool.user_pool_id}",
            jwt_audience=[user_pool_client.user_pool_client_id],
        )

        http_api = apigwv2.HttpApi(
            self,
            "HttpApi",
            api_name="reschene-api",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[apigwv2.CorsHttpMethod.GET, apigwv2.CorsHttpMethod.POST],
                allow_headers=["*"],
            ),
        )

        # --- Presigned URL Lambda ---
        presigned_url_fn = lambda_.Function(
            self,
            "PresignedUrlFunction",
            function_name="reschene-presigned-url",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/presigned_url"),
            memory_size=256,
            timeout=cdk.Duration.seconds(10),
            environment={
                "IMAGE_BUCKET": image_bucket.bucket_name,
                "PRESIGNED_EXPIRY": "900",
            },
        )
        image_bucket.grant_put(presigned_url_fn)

        http_api.add_routes(
            path="/upload/presigned-url",
            methods=[apigwv2.HttpMethod.POST],
            integration=apigwv2_int.HttpLambdaIntegration("PresignedUrlIntegration", presigned_url_fn),
            authorizer=authorizer,
        )

        # --- Search Lambda ---
        search_fn = lambda_.Function(
            self,
            "SearchFunction",
            function_name="reschene-search",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/search"),
            memory_size=256,
            timeout=cdk.Duration.seconds(60),
            environment={
                "ATHENA_WORKGROUP": workgroup_name,
                "GLUE_DATABASE": database_name,
                "GLUE_TABLE_RAW": raw_table_name,
                "GLUE_TABLE_COMPACTED": compacted_table_name,
                "ATHENA_RESULTS_BUCKET": athena_results_bucket.bucket_name,
            },
        )
        search_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=["*"],
            )
        )
        search_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"],
                resources=["*"],
            )
        )
        athena_results_bucket.grant_read_write(search_fn)
        search_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
                resources=[metadata_bucket.bucket_arn, f"{metadata_bucket.bucket_arn}/*"],
            )
        )

        http_api.add_routes(
            path="/search",
            methods=[apigwv2.HttpMethod.POST],
            integration=apigwv2_int.HttpLambdaIntegration("SearchIntegration", search_fn),
            authorizer=authorizer,
        )

        # --- Image URL Lambda ---
        image_url_fn = lambda_.Function(
            self,
            "ImageUrlFunction",
            function_name="reschene-image-url",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset("lambdas/image_url"),
            memory_size=256,
            timeout=cdk.Duration.seconds(10),
            environment={
                "IMAGE_BUCKET": image_bucket.bucket_name,
                "PRESIGNED_EXPIRY": "86400",
            },
        )
        image_bucket.grant_read(image_url_fn)

        http_api.add_routes(
            path="/images/url",
            methods=[apigwv2.HttpMethod.GET],
            integration=apigwv2_int.HttpLambdaIntegration("ImageUrlIntegration", image_url_fn),
            authorizer=authorizer,
        )

        # ================================================================
        # Outputs
        # ================================================================
        cdk.CfnOutput(self, "UserPoolId", value=user_pool.user_pool_id)
        cdk.CfnOutput(self, "UserPoolClientId", value=user_pool_client.user_pool_client_id)
        cdk.CfnOutput(
            self,
            "CognitoDomainUrl",
            value=f"https://reschene.auth.{self.region}.amazoncognito.com",
        )
        cdk.CfnOutput(
            self,
            "HostedUILoginUrl",
            value=(
                f"https://reschene.auth.{self.region}.amazoncognito.com/login"
                f"?client_id={user_pool_client.user_pool_client_id}"
                f"&response_type=code"
                f"&scope=openid+email+profile"
                f"&redirect_uri=http://localhost:3000/callback"
            ),
        )
        cdk.CfnOutput(self, "ApiEndpoint", value=http_api.api_endpoint)
        cdk.CfnOutput(self, "ImageBucketName", value=image_bucket.bucket_name)
        cdk.CfnOutput(self, "MetadataBucketName", value=metadata_bucket.bucket_name)
        cdk.CfnOutput(self, "ThumbnailBucketName", value=thumbnail_bucket.bucket_name)
        cdk.CfnOutput(self, "Output3DBucketName", value=output_3d_bucket.bucket_name)
        cdk.CfnOutput(self, "EcsClusterArn", value=cluster.cluster_arn)
        cdk.CfnOutput(self, "VpcId", value=vpc.vpc_id)
