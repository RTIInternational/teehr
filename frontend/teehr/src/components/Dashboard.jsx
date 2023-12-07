import { useState, useEffect } from "react";
import "../App.css";
import { Grid, Box, CircularProgress, Tab } from "@mui/material";
import { TabContext, TabList, TabPanel } from "@mui/lab";

import StationMap from "./StationMap";
import DataGridDemo from "./DataGrid";
import DashboardContext from "../Context.js";
import QuerySection from "./QueryStep.jsx";
import DisplayStep from "./DisplayStep.jsx";
import Results from "./Results.jsx";

function Dashboard() {
  const [errors, setErrors] = useState(false);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState({});

  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState("");

  const [metrics, setMetrics] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [displayMetric, setDisplayMetric] = useState("");

  const [groupByFields, setGroupByFields] = useState([]);
  const [selectedGroupByFields, setSelectedGroupByFields] = useState([]);

  const [operators, setOperators] = useState([]);

  const [filters, setFilters] = useState([]);
  const [includeSpatialData, setIncludeSpatialData] = useState(true);
  const [selectedTab, setSelectedTab] = useState("1");
  const [fieldValues, setFieldValues] = useState({});

  const addFilter = () => {
    setFilters([...filters, { column: "", operator: "", value: "" }]);
  };

  const updateFilter = (index, key, value) => {
    const newFilters = [...filters];
    newFilters[index][key] = value;
    setFilters(newFilters);
  };

  const deleteFilter = (indexToRemove) => {
    const newFilters = filters.filter((_, index) => index !== indexToRemove);
    setFilters(newFilters);
  };

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

  const [inputValues, setInputValues] = useState({});

  const contextValue = {
    data,
    displayMetric,
    datasets,
    metrics,
    groupByFields,
    fieldValues,
    filters,
    operators,
    errors,
    loading,
    includeSpatialData,
    selectedDataset,
    selectedMetrics,
    selectedGroupByFields,
    nonListFields,
    addFilter,
    updateFilter,
    deleteFilter,
    setDatasets,
    setMetrics,
    setGroupByFields,
    setFilters,
    setOperators,
    setErrors,
    setLoading,
    setFieldValues,
    setSelectedMetrics,
    setSelectedGroupByFields,
    setSelectedDataset,
    setDisplayMetric,
    setIncludeSpatialData,
    setData,
    inputValues,
    setInputValues,
    selectedTab,
    setSelectedTab,
  };

  useEffect(() => {
    console.log({ inputValues });
  }, [inputValues]);

  return (
    <DashboardContext.Provider value={contextValue}>
      <Box sx={{ p: 2 }}>
        <Grid container spacing={2}>
          <Grid item xs={5} sx={{ mt: 9 }}>
            <QuerySection />
            <DisplayStep />
            <Results />
          </Grid>
        </Grid>
      </Box>
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
    </DashboardContext.Provider>
  );
}

export default Dashboard;
