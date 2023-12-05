import { useState, useEffect } from "react";
import "../App.css";
import axios from "axios";
import {
  Button,
  Grid,
  Box,
  CircularProgress,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Typography,
} from "@mui/material";

import StationMap from "./StationMap";
import MetricSelect from "./MetricSelect.jsx";
import DatasetSelect from "./DatasetSelect.jsx";
import DisplayMetricSelect from "./DisplayMetricSelect.jsx";
import GroupBySelect from "./GroupBySelect";
import DataGridDemo from "./DataGrid";
import Filter from "./SingleFilter";
import DashboardContext from "../Context.js";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
// import MapWrapper from "./MapWrapper.jsx";

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

  const [filters, setFilters] = useState([
    { column: "", operator: "", value: "" },
  ]);
  const [includeSpatialData, setIncludeSpatialData] = useState(true);

  // const [orderByFields, setOrderByFields] = useState([]);
  // const [selectedOrderByFields, setSelectedOrderByFields] = useState([]);

  const formatGroupByFields = (fields) => {
    return fields.map((obj) => obj.name);
  };

  const fetchStations = () => {
    setLoading(true);
    axios
      .post(`http://localhost:8000/datasets/${selectedDataset}/get_metrics`, {
        group_by: formatGroupByFields(selectedGroupByFields),
        order_by: ["primary_location_id"],
        include_metrics: selectedMetrics,
        filters: filters,
        return_query: false,
        include_geometry: true,
      })
      .then((res) => {
        setData(res.data);
        setLoading(false);
      })
      .catch(function (err) {
        console.log(err);
        setErrors(err);
        setLoading(false);
      });
  };

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

  const locationField = groupByFields.find(
    (f) => f.name === "primary_location_id"
  );

  useEffect(() => {
    if (includeSpatialData && !selectedGroupByFields.includes(locationField)) {
      setSelectedGroupByFields((prev) => [...prev, locationField]);
    }
  }, [includeSpatialData, groupByFields, selectedGroupByFields, locationField]);

  const contextValue = {
    datasets,
    metrics,
    groupByFields,
    filters,
    operators,
    errors,
    loading,
    includeSpatialData,
    selectedDataset,
    selectedMetrics,
    selectedGroupByFields,
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
  };

  return (
    <DashboardContext.Provider value={contextValue}>
      <Box sx={{ flexGrow: 1 }}>
        <Grid container spacing={2}>
          <Grid item xs={5}>
            <Grid>
              <DatasetSelect
                datasets={datasets}
                setDatasets={setDatasets}
                setSelectedDataset={setSelectedDataset}
                selectedDataset={selectedDataset}
              />
            </Grid>
            {metrics && selectedDataset && (
              <Grid>
                <MetricSelect
                  metrics={metrics}
                  setMetrics={setMetrics}
                  selectedMetrics={selectedMetrics}
                  setSelectedMetrics={setSelectedMetrics}
                  selectedDataset={selectedDataset}
                />

                <GroupBySelect
                  groupByFields={groupByFields}
                  setGroupByFields={setGroupByFields}
                  selectedGroupByFields={selectedGroupByFields}
                  setSelectedGroupByFields={setSelectedGroupByFields}
                  selectedDataset={selectedDataset}
                  includeSpatialData={includeSpatialData}
                />
                <Box>
                  <Typography>Filters</Typography>
                  {filters.map((filter, index) => (
                    <div
                      key={index}
                      style={{ marginBottom: "8px", display: "flex" }}
                    >
                      <Filter
                        index={index}
                        selectedGroupByField={filter.column || ""}
                        selectedOperator={filter.operator || ""}
                        value={filter.value.toString() || ""}
                      />
                    </div>
                  ))}
                  <Button variant="standard" onClick={addFilter} color="grey">
                    <AddOutlinedIcon />
                  </Button>
                </Box>
                <Box sx={{ display: "flex", justifyContent: "center" }}>
                  <FormGroup>
                    <FormControlLabel
                      control={
                        <Checkbox
                          checked={includeSpatialData}
                          onChange={() =>
                            setIncludeSpatialData((prev) => !prev)
                          }
                          inputProps={{
                            "aria-label": "Include Spatial Data Option",
                          }}
                        />
                      }
                      label="Include Spatial Data"
                    ></FormControlLabel>
                  </FormGroup>
                  <Button
                    variant="contained"
                    onClick={() => {
                      fetchStations();
                    }}
                  >
                    Fetch Data
                  </Button>
                </Box>
              </Grid>
            )}
            <Grid>
              {selectedDataset && (
                <DisplayMetricSelect
                  selectedMetrics={selectedMetrics}
                  setSelectedDisplayMetric={setDisplayMetric}
                  selectedDisplayMetric={displayMetric}
                />
              )}
            </Grid>
          </Grid>
          <Grid item xs={7}>
            <Grid>
              {data && displayMetric && (
                <>
                  <StationMap stations={data} metricName={displayMetric} />
                  <DataGridDemo data={data} />
                </>
              )}
            </Grid>
            <Grid></Grid>
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
