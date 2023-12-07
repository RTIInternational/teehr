import React, { useState, useEffect } from "react";
import "@src/App.css";
import {
  Box,
  CircularProgress,
  Stepper,
  Step,
  StepLabel,
  Typography,
  Grid,
} from "@mui/material";
import DashboardContext from "../../Context.js";
import QuerySection from "./Step1Query.jsx";
import FiltersSection from "./Step2Filters.jsx";
import DisplayStep from "./Step3Results.jsx";
import useDashboardAPI from "../../hooks/useDashboardAPI.jsx";

function Dashboard() {
  const { loading, fetchStations, fetchDatasets } = useDashboardAPI();
  const [errors, setErrors] = useState(false);
  const [data, setData] = useState({});
  const [fieldOptions, setFieldOptions] = useState({});

  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState("");

  const [metrics, setMetrics] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [displayMetric, setDisplayMetric] = useState("");

  const [groupByFields, setGroupByFields] = useState([]);
  const [selectedGroupByFields, setSelectedGroupByFields] = useState([]);

  const [filters, setFilters] = useState([]);
  const [includeSpatialData, setIncludeSpatialData] = useState(true);
  const [selectedTab, setSelectedTab] = useState("1");
  const [fieldValues, setFieldValues] = useState({});

  const nonListFields = [
    "reference_time",
    "value_time",
    "secondary_value",
    "primary_value",
    "absolute_difference",
    "upstream_area_km2",
    "primary_normalized_discharge",
    "exceed_2yr_recurrence",
    "",
  ];

  const [activeStep, setActiveStep] = React.useState(0);
  const [skipped, setSkipped] = React.useState(new Set());
  const [operatorOptions, setOperatorOptions] = useState([]);

  const steps = ["Query", "Add Filters", "View Results"];

  const isStepOptional = (step) => {
    return step === 1;
  };

  const isStepSkipped = (step) => {
    return skipped.has(step);
  };

  const handleNext = () => {
    let newSkipped = skipped;
    if (isStepSkipped(activeStep)) {
      newSkipped = new Set(newSkipped.values());
      newSkipped.delete(activeStep);
    }

    setActiveStep((prevActiveStep) => prevActiveStep + 1);
    setSkipped(newSkipped);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleReset = () => {
    setActiveStep(0);
  };

  const handleFetchData = async (filters) => {
    let fields = [...selectedGroupByFields];
    if (includeSpatialData && !fields.includes("primary_location_id")) {
      fields.push("primary_location_id");
    }
    return fetchStations(
      selectedDataset,
      fields,
      selectedMetrics,
      filters
    ).then((res) => {
      setData(res.data);
      return;
    });
  };

  useEffect(() => {
    fetchDatasets().then((datasets) => {
      setDatasets(datasets);
    });
  }, []);

  const contextValue = {
    fieldOptions,
    setFieldOptions,
    operatorOptions,
    setOperatorOptions,
    data,
    displayMetric,
    datasets,
    metrics,
    groupByFields,
    fieldValues,
    filters,
    errors,
    loading,
    includeSpatialData,
    selectedDataset,
    selectedMetrics,
    selectedGroupByFields,
    nonListFields,
    setDatasets,
    setMetrics,
    setGroupByFields,
    setFilters,
    setErrors,
    setFieldValues,
    setSelectedMetrics,
    setSelectedGroupByFields,
    setSelectedDataset,
    setDisplayMetric,
    setIncludeSpatialData,
    setData,
    selectedTab,
    setSelectedTab,
    activeStep,
    handleFetchData,
  };

  return (
    <DashboardContext.Provider value={contextValue}>
      <Grid container sx={{ p: 4 }}>
        <Grid item xs={12} sx={{ mt: 4, width: "100%" }}>
          <Stepper activeStep={activeStep}>
            {steps.map((label, index) => {
              const stepProps = {};
              const labelProps = {};
              if (isStepOptional(index)) {
                labelProps.optional = (
                  <Typography variant="caption">Optional</Typography>
                );
              }
              if (isStepSkipped(index)) {
                stepProps.completed = false;
              }
              return (
                <Step key={label} {...stepProps}>
                  <StepLabel {...labelProps}>{label}</StepLabel>
                </Step>
              );
            })}
          </Stepper>
        </Grid>
        <Grid item xs={12} sx={{ mt: 8 }}>
          {activeStep === 0 && (
            <QuerySection onNext={handleNext} onBack={handleBack} />
          )}
          {activeStep === 1 && (
            <FiltersSection onNext={handleNext} onBack={handleBack} />
          )}
          {activeStep === 2 && (
            <DisplayStep onBack={handleBack} onReset={handleReset} />
          )}
        </Grid>
        {loading && (
          <Box
            display="flex"
            justifyContent="center"
            alignItems="center"
            position="fixed"
            top={0}
            left={0}
            width="100%"
            height="100%"
            bgcolor="rgba(255, 255, 255, 0.7)"
            zIndex="modal"
          >
            <CircularProgress />
          </Box>
        )}
      </Grid>
    </DashboardContext.Provider>
  );
}

export default Dashboard;
