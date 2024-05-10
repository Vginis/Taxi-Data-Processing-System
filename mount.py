# Mount Storage Account to Databricks
dbutils.fs.mount(
    source="wasbs://storagecontainer@taxibatchdata.blob.core.windows.net",
    mount_point="/mnt/taxidata",
    extra_configs={
        "fs.azure.account.key.taxibatchdata.blob.core.windows.net": "ejXy7zl57vPiu4WiNWZj8PMw996zSvPqVmlgMIDlppN2rJSpx03lSGMnLWPkS86CzJLOFgQA+eAe+AStxRlvnw=="
    },
)
