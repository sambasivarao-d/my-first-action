module "my-first-event-arc" {
  source                      = "./modules/event-arc"
  project_id                  = var.project_id
  eventarc-name               = var.eventarc-name
  prefix                      = var.prefix
  event-provider              = var.event-provider
  event-provider-service-name = var.event-provider-service-name
  provider-method-name-event  = var.provider-method-name-event
  event-destination           = var.event-destination
  region                      = var.region
  service_account             = var.service_account

}