import "@src/App.css";
import { TextField, Box, Button, Tab, Grid } from "@mui/material";
import DashboardContext from "../../Context.js";
import { useContext, useEffect, useState } from "react";
import { TabContext, TabList, TabPanel } from "@mui/lab";
import PropTypes from "prop-types";
import FormSingleSelect from "../Form/FormSingleSelect.jsx";
import { useForm } from "react-hook-form";
import StationMap from "../StationMap.jsx";
import DataGridDemo from "../DataGrid.jsx";
import FormInputText from "../Form/FormInputText.jsx";

const DisplayStep = (props) => {
  const { onBack, onReset } = props;
  const {
    queryOptions: { optionsForGroupByFields },
    userSelections: {
      selectedGroupByFields,
      selectedMetrics,
      selectedIncludeSpatialData,
    },
    fetchedData,
  } = useContext(DashboardContext);

  const [geoJSON, setGeoJSON] = useState({ features: [] });
  const [tabularData, setTabularData] = useState([]);
  const [selectedTab, setSelectedTab] = useState(
    selectedIncludeSpatialData ? "map" : "table"
  );
  const [displayMetric, setDisplayMetric] = useState(selectedMetrics[0] || "");
  const [groupByFilters, setGroupByFilters] = useState(() => {
    return selectedGroupByFields.reduce((acc, field) => {
      if (field !== "primary_location_id") {
        acc[field] =
          (optionsForGroupByFields[field] &&
            optionsForGroupByFields[field][0]) ||
          "";
      }
      return acc;
    }, {});
  });

  const filterDataByGroupByFilters = (data, groupByFilters) => {
    if (selectedIncludeSpatialData) {
      const filteredFeatures = data.features.filter((feature) => {
        return Object.entries(groupByFilters).every(([key, value]) => {
          if (!value) return true;
          return String(feature.properties[key]) === String(value);
        });
      });
      setGeoJSON({ features: filteredFeatures });
      setTabularData(filteredFeatures.map((feature) => feature.properties));
    } else {
      const filteredData = data.filter((d) => {
        return Object.entries(groupByFilters).every(([key, value]) => {
          if (!value) return true;
          return String(d[key]) === String(value);
        });
      });
      setTabularData(filteredData);
    }
  };

  const handleTabChange = (e, value) => {
    setSelectedTab(value);
  };

  const handleDisplayMetricChange = (newDisplayMetric, onChange) => {
    setDisplayMetric(newDisplayMetric);
    onChange(newDisplayMetric);
  };

  const handleGroupFilterChange = (newValue, onChange, field) => {
    setGroupByFilters((prev) => ({ ...prev, [field]: newValue }));
    filterDataByGroupByFilters(fetchedData, {
      ...groupByFilters,
      [field]: newValue,
    });
    onChange(newValue);
  };

  const { control } = useForm({
    defaultValues: {
      displayMetric: selectedMetrics[0] || "",
      ...groupByFilters,
    },
  });

  useEffect(() => {
    filterDataByGroupByFilters(fetchedData, groupByFilters);
  }, []);

  return (
    <>
      {(!fetchedData || fetchedData.length === 0) && (
        <div>Response returned no data.</div>
      )}
      <form>
        <Grid container>
          <Grid item xs={12} md={5} sx={{ mt: 9 }}>
            <FormSingleSelect
              name={"displayMetric"}
              control={control}
              label={"Select Display Metric"}
              options={selectedMetrics || []}
              onChange={handleDisplayMetricChange}
              formStyle={{ m: 0 }}
            />
            {Object.keys(groupByFilters).map((field, index) => (
              <Box
                key={index}
                sx={{
                  display: "flex",
                }}
              >
                <TextField
                  size="small"
                  value={field}
                  disabled
                  sx={{ m: 0.5, width: "50%" }}
                />
                {field in optionsForGroupByFields && (
                  <FormSingleSelect
                    name={`${field}`}
                    control={control}
                    label={"Value"}
                    options={optionsForGroupByFields[field] || []}
                    rules={{ required: "Required." }}
                    onChange={(e, fn) => handleGroupFilterChange(e, fn, field)}
                  />
                )}
                {!(field in optionsForGroupByFields) && (
                  <FormInputText
                    name={`${field}`}
                    control={control}
                    label={"Value"}
                    options={optionsForGroupByFields[field] || []}
                    rules={{ required: "Required." }}
                    onChange={(e, fn) => handleGroupFilterChange(e, fn, field)}
                  />
                )}
              </Box>
            ))}
          </Grid>
          <Grid item xs={12} md={7}>
            {fetchedData && Object.keys(fetchedData).length > 0 && (
              <TabContext value={selectedTab}>
                <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
                  <TabList
                    onChange={handleTabChange}
                    aria-label="Dashboard tabs"
                    variant="fullWidth"
                  >
                    <Tab
                      label="Map"
                      value="map"
                      disabled={!selectedIncludeSpatialData}
                    />
                    <Tab label="Table" value="table" />
                  </TabList>
                </Box>
                <TabPanel value="map">
                  {Object.keys(geoJSON.features).length > 0 && (
                    <StationMap stations={geoJSON} metricName={displayMetric} />
                  )}
                </TabPanel>
                <TabPanel value="table">
                  {Object.keys(tabularData).length > 0 && (
                    <DataGridDemo data={tabularData} />
                  )}
                </TabPanel>
              </TabContext>
            )}
          </Grid>
        </Grid>
      </form>

      <Box sx={{ display: "flex", flexDirection: "row", pt: 2 }}>
        <Button color="inherit" onClick={onBack} sx={{ mr: 1 }}>
          Back
        </Button>
        <Box sx={{ flex: "1 1 auto" }} />
        <Button onClick={onReset}>Reset</Button>
      </Box>
    </>
  );
};
DisplayStep.propTypes = {
  onReset: PropTypes.func.isRequired,
  onBack: PropTypes.func.isRequired,
};
export default DisplayStep;
