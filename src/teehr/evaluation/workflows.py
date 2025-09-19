"""A class for evaluation workflows."""
from teehr.models.workflows.base import WorkflowModelBase
from teehr.models.workflows.ngiab_teehr import NGIABWorkflowModel


class Workflow:
    """A base class for TEEHR evaluation workflows."""

    def __init__(self, ev):
        """Initialize the Workflow class."""
        self.ev = ev

    def ngiab_teehr(
        self,
        workflow_model: WorkflowModelBase = NGIABWorkflowModel
    ):
        """Run the workflow to calculate metrics from NGIAB output."""
        workflow_instance = workflow_model(ev=self.ev)
        workflow_instance.execute()

    def custom(
        self,
        workflow_model: WorkflowModelBase
    ):
        """Run a user-defined workflow."""
        workflow_instance = workflow_model(ev=self.ev)
        workflow_instance.execute()

    # Could have other canned workflows like:
    # 1. Populate an Evaluation with local data
    # 2. Populate an Evaluation with USGS and NWM data
    # 3. Run some canned metric workflow on an existing Evaluation
    # 4. Run some ensemble analyses on an existing Evaluation
    # 5. For the custom method, users could create their own instance of
    #    a WorkflowModelBase and pass it in.
    # 6. others...?
