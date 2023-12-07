import { useState, useContext } from "react";
import "../App.css";
import { Grid, Box, CircularProgress, Tab } from "@mui/material";
import { TabContext, TabList, TabPanel } from "@mui/lab";

import StationMap from "./StationMap";
import DataGridDemo from "./DataGrid";
import DashboardContext from "../Context.js";
import QuerySection from "./QueryStep.jsx";
import DisplayStep from "./DisplayStep.jsx";

const Results = () => {
  const {
    data,
    displayMetric,
    includeSpatialData,
    selectedTab,
    setSelectedTab,
  } = useContext(DashboardContext);
  const handleTabChange = (e, value) => {
    setSelectedTab(value);
  };
  console.log({ data, displayMetric });
  return (
    <Grid>
      {data && displayMetric && (
        <TabContext value={selectedTab}>
          <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
            <TabList
              onChange={handleTabChange}
              aria-label="Dashboard tabs"
              variant="fullWidth"
            >
              <Tab label="Map" value="1" disabled={!includeSpatialData} />
              <Tab label="Table" value="2" />
            </TabList>
          </Box>
          <TabPanel value="1">
            <StationMap stations={data} metricName={displayMetric} />
          </TabPanel>
          <TabPanel value="2">
            <DataGridDemo data={data} />
          </TabPanel>
        </TabContext>
      )}
    </Grid>
  );
};

export default Results;
