resource "google_eventarc_trigger" "trigger_auditlog_tf" {
  name     = "${var.prefix}-${var.eventarc-name}"
  location = var.region
  project  = var.project_id
  matching_criteria {
    attribute = "type"
    value     = var.event-provider
  }
  matching_criteria {
    attribute = "serviceName"
    value     = var.event-provider-service-name
  }
  matching_criteria {
    attribute = "methodName"
    value     = var.provider-method-name-event
  }
  destination {
    cloud_run_service {
      service = var.event-destination
      region  = var.region
    }
  }
  service_account = var.service_account
}

