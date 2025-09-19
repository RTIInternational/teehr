"""TEEHR NGIAB workflow model configuration."""
from teehr.models.workflows.base import WorkflowModelBase


class NGIABWorkflowModel(WorkflowModelBase):
    """NGIAB workflow model configuration."""

    def execute(self):
        """Execute the NGIAB workflow.

        Reproduce steps from: https://github.com/CIROH-UA/ngiab-teehr/blob/main/scripts/teehr_ngen.py

        - Get start/end dates from ngen
        - Get crosswalk and usgs point geometry
        - Get USGS gages from hydrofabric
        - Get ngen output and map to usgs and nwm IDs
        - Load locations
        - Load crosswalk
        - Add configuration for ngen
        - Fetch and load nwm v3.0 retro
        - Load ngen data
        - Fetch and load USGS data
        - Create joined timeseries
        - Query canned metrics
        - Save output to csv

        Basically almost the entire teehr_ngen.py script could be replaced
        with ev.workflows.ngiab_teehr()
        """
        pass
