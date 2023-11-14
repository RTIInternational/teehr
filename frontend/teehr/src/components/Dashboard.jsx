import { useState } from "react";
import "../App.css";
import axios from "axios";
// import Box from '@mui/material/Box';
// import Grid from '@mui/material/Grid';
import { Button, Grid, Box } from "@mui/material";

import StationMap from "./StationMap";
import MetricSelect from "./MetricSelect.jsx";
import DatasetSelect from "./DatasetSelect.jsx";
import DisplayMetricSelect from "./DisplayMetricSelect.jsx";
import GroupBySelect from "./GroupBySelect";
import DataGridDemo from "./DataGrid";
import Filters from "./Filters";
import DashboardContext from "../Context.js";
// import MapWrapper from "./MapWrapper.jsx";

function Dashboard() {
  const [errors, setErrors] = useState(false);
  const [loading, setLoading] = useState(true);
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

  // const [orderByFields, setOrderByFields] = useState([]);
  // const [selectedOrderByFields, setSelectedOrderByFields] = useState([]);

  const formatGroupByFields = (fields) => {
    return fields.map((obj) => obj.name);
  };

  const fetchStations = () => {
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
      })
      .catch(function (err) {
        console.log(err);
        setErrors(err);
      });
  };

  const contextValue = {
    datasets,
    metrics,
    groupByFields,
    filters,
    operators,
    errors,
    loading,
    selectedDataset,
    selectedMetrics,
    selectedGroupByFields,
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
                />
                <Filters />

                <Button
                  variant="contained"
                  onClick={() => {
                    fetchStations();
                  }}
                >
                  Fetch Data
                </Button>
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
    </DashboardContext.Provider>
  );
}

export default Dashboard;
