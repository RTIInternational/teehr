"""Tests for interacting with the remote EaaS service."""
from teehr.models.pydantic_table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
import tempfile

CATALOG_URI = "http://dev-teehr-sys-iceberg-alb-2105268770.us-east-2.elb.amazonaws.com"
WAREHOUSE_PATH = "s3://dev-teehr-sys-iceberg-warehouse/warehouse/"


def test_eaas_interface(tmpdir):
    """Test creating a new study."""
    from teehr import Evaluation

    ev = Evaluation(
        local_warehouse_dir=tmpdir,
        create_local_dir=True,
        catalog_name="iceberg",
        catalog_uri=CATALOG_URI,
        catalog_type="rest",
        local_namespace_name="teehr",  # aka namespace, database
        remote_warehouse_dir=WAREHOUSE_PATH,
        check_evaluation_version=False,
    )

    # NOTE: It might take a few seconds for the catalog to be ready?
    ev.spark.catalog.listCatalogs()
    ev.spark.catalog.currentCatalog()
    ev.spark.catalog.listDatabases()
    ev.spark.catalog.listTables("teehr")

    print(ev.list_tables())

    ev.clone_template()

    # Check configurations.add doesn't add columns
    cols = ev.configurations.to_pandas().columns

    ev.configurations.add(
        configuration=[
            Configuration(
                name="conf1",
                type="secondary",
                description="Configuration 1",
            )
        ]
    )

    df = ev.configurations.to_pandas()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_eaas_interface(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )