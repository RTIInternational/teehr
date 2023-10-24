import { useState, useEffect } from 'react'
import './App.css'
import axios from "axios";
// import Box from '@mui/material/Box';
// import Grid from '@mui/material/Grid';
import { Button, Grid, Box, Typography } from '@mui/material';

import StationMap from './components/StationMap';
import MetricSelect from './components/MetricSelect.jsx'
import DatasetSelect from './components/DatasetSelect.jsx';
import DisplayMetricSelect from './components/DisplayMetricSelect.jsx'
import GroupBySelect from './components/GroupBySelect';
import DataGridDemo from './components/DataGrid';
// import MapWrapper from "./MapWrapper.jsx";

function App() {
  const [errors, setErrors] = useState(false);
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState({});

  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState("");

  const [metrics, setMetrics] = useState([]);
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [displayMetric, setDisplayMetric] = useState("")


  const [groupByFields, setGroupByFields] = useState([]);
  const [selectedGroupByFields, setSelectedGroupByFields] = useState([]);

  // const [orderByFields, setOrderByFields] = useState([]);
  // const [selectedOrderByFields, setSelectedOrderByFields] = useState([]);

  const formatGroupByFields = (fields) => {
    return fields.map(obj => obj.name)
  }

  const fetchStations = () => {
    axios
      .post(`http://localhost:8000/datasets/${selectedDataset}/get_metrics`,
        {
          group_by: formatGroupByFields(selectedGroupByFields),
          order_by: ["primary_location_id"],
          include_metrics: selectedMetrics,
          return_query: false,
          include_geometry: true
        }
      )
      .then((res) => {
        console.log(res.data);
        setData(res.data);
      })
      .catch(function (err) {
        console.log(err);
        setErrors(err);
      });
  }

  return (
    <Box sx={{ flexGrow: 1 }}>
      <Grid container spacing={2}>
        <Grid item xs={4}>
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

              <Typography>Filters</Typography>


              <Button variant="contained" onClick={() => { fetchStations() }}>Fetch Data</Button>

            </Grid>
          )}
          <Grid>
            {selectedDataset &&
              <DisplayMetricSelect selectedMetrics={selectedMetrics} setSelectedDisplayMetric={setDisplayMetric} selectedDisplayMetric={displayMetric} />
            }
          </Grid>
        </Grid>
        <Grid item xs={8}>
          <Grid>
          {
            (data && displayMetric) && (
              <>
            <StationMap stations={data} metricName={displayMetric} />
            <DataGridDemo data={data}/>
              </>
            )
          }
          </Grid>
          <Grid>

          </Grid>
        </Grid>
      </Grid>
    </Box>

  )
}

export default App
