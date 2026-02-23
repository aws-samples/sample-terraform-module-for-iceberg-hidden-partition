# https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/id
resource "random_id" "main" {
  for_each = { "enabled" = true }

  byte_length = 2
}

# https://registry.terraform.io/providers/hashicorp/time/latest/docs/resources/static
resource "time_static" "main" {
  for_each = { "enabled" = true }
}

# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_database
resource "aws_glue_catalog_database" "main" {
  for_each = { "enabled" = true }

  name         = local.name_base
  description  = var.description
  location_uri = var.location_uri

  dynamic "create_table_default_permission" {
    for_each = var.create_table_default_permission == null ? {} : { "enabled" = var.create_table_default_permission }

    content {
      permissions = create_table_default_permission.value.permissions

      dynamic "principal" {
        for_each = create_table_default_permission.value.principal == null ? {} : { "enabled" = create_table_default_permission.value.principal }

        content {
          data_lake_principal_identifier = principal.value.data_lake_principal_identifier
        }
      }
    }
  }
}

# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_table
resource "aws_glue_catalog_table" "main" {
  for_each = var.tables

  name               = each.value.name
  database_name      = aws_glue_catalog_database.main["enabled"].name
  table_type         = each.value.table_type
  parameters         = contains(["iceberg", "ICEBERG"], lookup(each.value.parameters, "table_type", "")) ? { for k, v in each.value.parameters : k => v if k != "table_type" } : each.value.parameters
  view_expanded_text = each.value.table_type == "VIRTUAL_VIEW" && each.value.view_expanded_text != null ? each.value.view_expanded_text : null
  view_original_text = each.value.table_type == "VIRTUAL_VIEW" && each.value.view_original_text != null ? each.value.view_original_text : null

  # Support for IceBerg tables
  dynamic "open_table_format_input" {
    for_each = contains(["iceberg", "ICEBERG"], lookup(each.value.parameters, "table_type", "")) ? { "enabled" = true } : {}

    content {
      iceberg_input {
        metadata_operation = "CREATE"
        version            = "2"
      }
    }
  }

  dynamic "partition_keys" {
    for_each = each.value.partition_keys == null ? {} : each.value.partition_keys

    content {
      name    = partition_keys.key
      type    = partition_keys.value.type
      comment = partition_keys.value.comment == null ? "" : partition_keys.value.comment
    }
  }

  dynamic "storage_descriptor" {
    for_each = each.value.storage_descriptor == null ? {} : { "enabled" = each.value.storage_descriptor }

    content {
      location      = each.value.table_type != "VIRTUAL_VIEW" ? storage_descriptor.value.location : null
      input_format  = each.value.table_type != "VIRTUAL_VIEW" ? storage_descriptor.value.input_format : null
      output_format = each.value.table_type != "VIRTUAL_VIEW" ? storage_descriptor.value.output_format : null

      dynamic "ser_de_info" {
        for_each = storage_descriptor.value.ser_de_info == null ? {} : { "enabled" = storage_descriptor.value.ser_de_info }

        content {
          name                  = ser_de_info.value.name
          serialization_library = ser_de_info.value.serialization_library
          parameters            = ser_de_info.value.parameters
        }
      }

      dynamic "columns" {
        for_each = storage_descriptor.value.columns

        content {
          name    = columns.value.name
          type    = columns.value.type
          comment = columns.value.comment
        }
      }
    }
  }
}

resource "terraform_data" "iceberg_hidden_partition" {
  for_each = local.partitioned_iceberg_tables

  triggers_replace = {
    table_definition = jsonencode(each.value.partition_transforms)
  }

  lifecycle {
    precondition {
      condition     = var.deploy_role_arn != null
      error_message = "deploy_role_arn is required when partitioning is needed for Iceberg tables."
    }
  }

  provisioner "local-exec" {
    command = <<-EOT
      export $(aws sts assume-role --role-arn ${var.deploy_role_arn} --role-session-name iceberg-schema-change --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text | awk '{print "AWS_ACCESS_KEY_ID="$1" AWS_SECRET_ACCESS_KEY="$2" AWS_SESSION_TOKEN="$3}') && \
      TEMP_DIR=$(mktemp -d) && \
      pip3 install --target $TEMP_DIR boto3 && \
      PYTHONPATH=$TEMP_DIR python3 ${path.module}/scripts/iceberg_hidden_partition.py \
        --database ${aws_glue_catalog_database.main["enabled"].name} \
        --table ${each.value.name} \
        --partitions-json '${jsonencode(each.value.partition_transforms)}' \
        --glue-job ${local.temp_glue_job_name} \
        --glue-job-role ${var.deploy_role_arn} \
        --script-path ${path.module}/resources/glue/partition.py \
        --s3-output ${var.iceberg_schema_config.athena_s3_output} \
        --region ${data.aws_region.current.name} && \
      rm -rf $TEMP_DIR
    EOT
  }
}