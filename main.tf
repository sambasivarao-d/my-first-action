provider "google"{}
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