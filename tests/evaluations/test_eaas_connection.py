"""Tests for interacting with the remote EaaS service."""
from teehr.models.pydantic_table_models import Configuration
from teehr import Evaluation
import tempfile


def test_eaas_interface(tmpdir):
    """Test creating a new study."""
    # Connects to remote by default unless otherwise specified.
    ev = Evaluation(
        local_warehouse_dir=tmpdir,
        create_local_dir=True,
        check_evaluation_version=False
    )
    # NOTE: It might take a few seconds for the catalog to be ready?
    ev.spark.catalog.listCatalogs()
    ev.spark.catalog.currentCatalog()

    ev.clone_template()

    ev.spark.catalog.listDatabases()
    ev.spark.catalog.listTables("teehr")
    tbls_df = ev.list_tables()

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
    assert len(df) == 1
    assert df.columns.equals(cols)
    assert len(tbls_df) == 10


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