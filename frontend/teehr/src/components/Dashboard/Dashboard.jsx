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
import { defaultFormState } from "./constants";

function Dashboard() {
  const { loading, errors, fetchDatasets } = useDashboardAPI();

  const [userSelections, setUserSelections] = useState({ ...defaultFormState });
  const [datasets, setDatasets] = useState([]);
  const [metrics, setMetrics] = useState([]);
  const [groupByFields, setGroupByFields] = useState([]);
  const [operators, setOperators] = useState([]);
  const [optionsForGroupByFields, setOptionsForGroupByFields] = useState({});

  const [fetchedData, setFetchedData] = useState({});

  const [activeStep, setActiveStep] = React.useState(0);

  const steps = ["Query", "Add Filters", "View Results"];

  const isStepOptional = (step) => {
    return step === 1;
  };

  const handleNext = () => {
    setActiveStep((prevActiveStep) => prevActiveStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevActiveStep) => prevActiveStep - 1);
  };

  const handleReset = () => {
    setUserSelections(defaultFormState);
    setActiveStep(0);
  };

  useEffect(() => {
    fetchDatasets().then((datasets) => {
      setDatasets(datasets);
    });
  }, []);

  const contextValue = {
    queryOptions: {
      datasets,
      metrics,
      groupByFields,
      operators,
      optionsForGroupByFields,
    },
    actions: {
      setUserSelections,
      setDatasets,
      setOptionsForGroupByFields,
      setOperators,
      setMetrics,
      setGroupByFields,
      setFetchedData,
    },
    userSelections,
    fetchedData,
  };

  if (errors) {
    return (
      <>
        <div>An error occurred:</div>
        <div> {errors?.message}</div>
      </>
    );
  }

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
