import React from "react";
import "../App.css";
import axios from "axios";
import {
  Button,
  Grid,
  Box,
  Checkbox,
  FormControlLabel,
  FormGroup,
  Typography,
} from "@mui/material";
import MetricSelect from "./MetricSelect.jsx";
import DatasetSelect from "./DatasetSelect.jsx";
import GroupBySelect from "./GroupBySelect.jsx";
import Filter from "./SingleFilter.jsx";
import DashboardContext from "../Context.js";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";

const QuerySection = () => {
  const {
    addFilter,
    metrics,
    selectedDataset,
    filters,
    includeSpatialData,
    groupByFields,
    setSelectedGroupByFields,
    selectedGroupByFields,
    setLoading,
    setData,
    selectedMetrics,
    setErrors,
    setIncludeSpatialData,
  } = React.useContext(DashboardContext);

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
        console.log({ res });
        setData(res.data);
        setLoading(false);
      })
      .catch(function (err) {
        console.log(err);
        setErrors(err);
        setLoading(false);
      });
  };

  const locationField = groupByFields.find(
    (f) => f.name === "primary_location_id"
  );

  const handleCheckboxChange = () => {
    const checked = !includeSpatialData;
    if (checked && !selectedGroupByFields.includes(locationField)) {
      setSelectedGroupByFields((prev) => [...prev, locationField]);
    }
    setIncludeSpatialData((prev) => !prev);
  };
  return (
    <Grid>
      <DatasetSelect />
      {metrics && selectedDataset && (
        <Grid>
          <MetricSelect />
          <GroupBySelect />
          <Box
            sx={{
              display: "flex",
              justifyContent: "center",
              alignItems: "center",
              m: 2,
            }}
          >
            <Typography>Filters</Typography>
            <Button variant="standard" onClick={addFilter} color="grey">
              <AddOutlinedIcon />
            </Button>
          </Box>
          {filters.map((filter, index) => (
            <div key={index} style={{ display: "flex" }}>
              <Filter
                index={index}
                selectedGroupByField={filter.column || ""}
                selectedOperator={filter.operator || ""}
                value={filter.value.toString() || ""}
              />
            </div>
          ))}
          <Box sx={{ display: "flex", justifyContent: "center" }}>
            <FormGroup>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={includeSpatialData}
                    onChange={handleCheckboxChange}
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
    </Grid>
  );
};

export default QuerySection;
