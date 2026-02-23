variable "required_tags" {
  description = "Standard tags for all resources."
  type = object(
    {
      CapabilityName       = string
      CostCenter           = string
      DeploymentRepository = string
      Environment          = string
      OwnerEmail           = string
      OwnerName            = string
      ProductName          = string
    }
  )
}

variable "additional_tags" {
  description = "Additional tags to assign to all resources. These tags will be ignored if they conflict with required tags."
  type        = map(string)
  default     = {}
}

variable "name_suffix" {
  description = "An additional string to include near the end of all resource names to futher group like resources and prevent name collisions."
  type        = string
  default     = ""

  validation {
    condition     = length(var.name_suffix) == 0 ? true : can(regex("^(\\w)+$", var.name_suffix))
    error_message = "Special characters other than underscore (_) are not supported for Glue database names."
  }
}

variable "description" {
  description = "Description of the database."
  type        = string
  default     = null
}

variable "location_uri" {
  description = "Location of the database (for example, an HDFS path)."
  type        = string
  default     = null
}

variable "create_table_default_permission" {
  description = "Creates a set of default permissions on the table for principals."
  type = object({
    permissions = list(string)

    principal = object({
      data_lake_principal_identifier = string
    })
  })

  default = null
}

variable "tables" {
  description = "A map of Glue table objects to attach to Glue tables. For name of the table. For Hive compatibility, this must be entirely lowercase."
  type = map(object({
    name               = string
    table_type         = string
    parameters         = map(any)
    view_expanded_text = optional(string)
    view_original_text = optional(string)

    partition_keys = optional(map(object({
      type    = string
      comment = optional(string)
    })))

    partition_transforms = optional(map(string))

    storage_descriptor = optional(object({
      location      = optional(string)
      input_format  = optional(string)
      output_format = optional(string)

      ser_de_info = optional(object({
        name                  = string
        serialization_library = string
        parameters            = map(any)
      }))

      columns = optional(map(object({
        name    = string
        type    = string
        comment = string
        })
      ))
    }))
  }))

  default = {}

  validation {
    condition     = length(var.tables) == 0 ? true : alltrue([for tbl in var.tables : can(regex("^(\\w)+$", tbl.name))])
    error_message = "Special characters other than underscore (_) are not supported for Glue table names."
  }
}

variable "iac_overrides" {
  description = "Enable logic overrides for unique scenarios."
  default     = {}
  type = object({
    explicit_db_name     = optional(bool, false)
    prefix_explicit_name = optional(bool, true)
  })
}

variable "deploy_role_arn" {
  description = "Deploy role ARN for assuming permissions to run schema changes."
  type        = string
  default     = null
}

variable "iceberg_schema_config" {
  description = "Configuration for Iceberg schema change detection and management."
  type = object({
    athena_s3_output       = optional(string, "")
    partition_glue_job_role = optional(string, "")
  })
  default = {
    athena_s3_output       = ""
    partition_glue_job_role = ""
  }
}

