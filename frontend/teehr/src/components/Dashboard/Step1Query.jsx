import React from "react";
import "@src/App.css";
import { Button, Box, Grid } from "@mui/material";
import DashboardContext from "../../Context.js";
import FormCheckbox from "../Form/FormCheckbox.jsx";
import { useForm } from "react-hook-form";
import useDashboardAPI from "../../hooks/useDashboardAPI.jsx";
import FormMultipleSelect from "../Form/FormMultipleSelect.jsx";
import FormSingleSelect from "../Form/FormSingleSelect.jsx";
import PropTypes from "prop-types";
import { nonListFields } from "./constants";

const QuerySection = (props) => {
  const { onNext } = props;
  const {
    fetchMetricFields,
    fetchGroupByFields,
    fetchFilterOperators,
    fetchOptionsForMultipleFields,
  } = useDashboardAPI();
  const {
    queryOptions: { datasets, metrics, groupByFields, optionsForGroupByFields },
    userSelections: {
      selectedDataset,
      selectedMetrics,
      selectedGroupByFields,
      selectedIncludeSpatialData,
    },
    actions: {
      setMetrics,
      setGroupByFields,
      setOperators,
      setOptionsForGroupByFields,
      setUserSelections,
    },
  } = React.useContext(DashboardContext);

  const defaultValues = {
    selectedDataset: selectedDataset || "",
    selectedMetrics: selectedMetrics || [],
    selectedGroupByFields: selectedGroupByFields || [],
    selectedIncludeSpatialData: selectedIncludeSpatialData,
  };

  const { handleSubmit, control } = useForm({ defaultValues });

  const onSubmit = async () => {
    await getFieldOptions();
    onNext();
  };

  const handleDatasetChange = (newDataset, onChange) => {
    fetchMetricFields(newDataset).then((metrics) => {
      setMetrics(metrics);
    });
    fetchGroupByFields(newDataset).then((fields) => {
      setGroupByFields(fields);
    });
    fetchFilterOperators().then((res) => {
      setOperators(res);
    });
    setUserSelections((prev) => ({ ...prev, selectedDataset: newDataset }));
    onChange(newDataset);
  };

  const handleMetricChange = (newMetrics, onChange) => {
    setUserSelections((prev) => ({ ...prev, selectedMetrics: newMetrics }));
    onChange(newMetrics);
  };

  const handleGroupByFieldChange = (newGroupByFields, onChange) => {
    setUserSelections((prev) => ({
      ...prev,
      selectedGroupByFields: newGroupByFields,
    }));
    onChange(newGroupByFields);
  };

  const handleSpatialDataChange = (newIncludeSpatialData, onChange) => {
    setUserSelections((prev) => ({
      ...prev,
      selectedIncludeSpatialData: newIncludeSpatialData,
    }));
    onChange(newIncludeSpatialData);
  };

  const getFieldOptions = () => {
    const fieldsToFetch = selectedGroupByFields.filter((field) => {
      return !(
        field in optionsForGroupByFields || nonListFields.includes(field)
      );
    });
    if (fieldsToFetch.length === 0) {
      return;
    }
    fetchOptionsForMultipleFields(fieldsToFetch, selectedDataset).then(
      (fieldValues) => {
        setOptionsForGroupByFields((prev) => {
          return { ...prev, ...fieldValues };
        });
      }
    );
  };

  return (
    <form onSubmit={handleSubmit(onSubmit)}>
      <Grid container justifyContent="center">
        <Grid item xs={12} sm={8} md={6}>
          <FormSingleSelect
            name={"selectedDataset"}
            control={control}
            label={"Select Dataset"}
            options={datasets || []}
            onChange={handleDatasetChange}
            rules={{ required: "Required." }}
          />
          {selectedDataset && (
            <>
              <FormMultipleSelect
                name="selectedMetrics"
                control={control}
                label="Select Metrics"
                options={metrics || []}
                onChange={handleMetricChange}
                rules={{ required: "Required." }}
              />
              <FormMultipleSelect
                name="selectedGroupByFields"
                control={control}
                label="Group By Fields"
                options={groupByFields.map((f) => f.name) || []}
                onChange={handleGroupByFieldChange}
                rules={{ required: "Required." }}
              />
              <FormCheckbox
                name="selectedIncludeSpatialData"
                control={control}
                label="Include Spatial Data"
                onChange={handleSpatialDataChange}
              />
            </>
          )}
        </Grid>
      </Grid>
      <Box sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
        <Box sx={{ flex: "1 1 auto" }} />
        <Button type="submit">Next</Button>
      </Box>
    </form>
  );
};

QuerySection.propTypes = {
  onNext: PropTypes.func.isRequired,
};

export default QuerySection;
