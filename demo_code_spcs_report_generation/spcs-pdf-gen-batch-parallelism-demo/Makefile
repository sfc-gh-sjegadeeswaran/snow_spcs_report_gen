SNOWFLAKE_REPO=<orgname>-<accountname>.registry.snowflakecomputing.com/<db>/<schema>/<reponame>
SNOWFLAKE_URL=<orgname>-<accountname>.registry.snowflakecomputing.com
DOCKER_IMAGE=pdf_gen_demo_batchjob_parallel_image
SNOWFLAKE_COMPUTE_POOL=spcs_compute_pool
SNOWFLAKE_CONNECTION=<connection string from toml file>

login:         ## Login to Snowflake Docker repo
	snow spcs image-registry token --connection $(SNOWFLAKE_CONNECTION) --format=JSON | docker login $(SNOWFLAKE_URL) --username 0sessiontoken --password-stdin

build:         ## Build Docker image for Snowpark Container Services
	docker build --platform linux/amd64 -t $(SNOWFLAKE_REPO)/$(DOCKER_IMAGE) .

push_docker:   ## Push Docker image to Snowpark Container Services
	docker push $(SNOWFLAKE_REPO)/$(DOCKER_IMAGE)

ddl:           ## just for demo to show that DDL can be generated here and deployed on to Snowflake
	@printf "CREATE SERVICE st_spcs\n"
	@printf "  IN COMPUTE POOL  $(SNOWFLAKE_COMPUTE_POOL)\n"
	@printf "  FROM SPECIFICATION \$$\$$\n"
	@printf "\n  \$$\$$;\n"
