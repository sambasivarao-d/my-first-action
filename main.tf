module "bigquery" {
  source       = "./modules/bigquery"
  dataset_id   = "myfirstdataset"
  dataset_name = "myfirstdataset"
  description  = "my first dataset creating to test github actions"
  project_id   = "dssrc-test-project-367514"
  location     = "US"

  tables = [
    {
      table_id           = "bq_raw"
      schema             = ""
      time_partitioning  = null
      range_partitioning = null
      expiration_time    = null
      clustering         = []
      labels             = {}

    },
    {
      table_id           = "bq_spd"
      schema             = ""
      time_partitioning  = null
      range_partitioning = null
      expiration_time    = null
      clustering         = []
      labels             = {}
    },
    {
      table_id           = "bq_failed"
      schema             = ""
      time_partitioning  = null
      range_partitioning = null
      expiration_time    = null
      clustering         = []
      labels             = {}
    },
  ]
}

module "cloud_run" {
  source       = "./modules/cloud-run"
  service_name = "my-first-cloud-run-service"
  project_id   = "dssrc-test-project-367514"
  location     = "us-central1"
  image        = "gcr.io/cloudrun/hello"
}

resource "docker_registry_image" "helloworld" {
  name = "helloworld:1.0"

  build {
    context = "${path.cwd}/absolutePathToContextFolder"
  }
}
