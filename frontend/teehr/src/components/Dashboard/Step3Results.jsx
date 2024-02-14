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
  const { formData, fieldOptions, data } = useContext(DashboardContext);

  const { selectedGroupByFields, selectedMetrics, includeSpatialData } =
    formData;

  const [geoJSON, setGeoJSON] = useState({ features: [] });
  const [tabularData, setTabularData] = useState([]);
  const [selectedTab, setSelectedTab] = useState(
    includeSpatialData ? "1" : "2"
  );
  const [displayMetric, setDisplayMetric] = useState("");
  const [groupByFilters, setGroupByFilters] = useState(() => {
    return selectedGroupByFields.reduce((obj, item) => {
      obj[item] = (fieldOptions[item] && fieldOptions[item][0]) || "";
      return obj;
    }, {});
  });

  const filterDataByGroupByFilters = (data, groupByFilters) => {
    if (includeSpatialData) {
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
    filterDataByGroupByFilters(data, { ...groupByFilters, [field]: newValue });
    onChange(newValue);
  };

  const { control } = useForm({
    defaultValues: {
      displayMetric: selectedMetrics[0] || "",
      ...groupByFilters,
    },
  });

  useEffect(() => {
    filterDataByGroupByFilters(data, groupByFilters);
  }, []);

  return (
    <>
      {(!data || data.length === 0) && <div>Response returned no data.</div>}
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
            {selectedGroupByFields.map((field, index) => (
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
                {field in fieldOptions && (
                  <FormSingleSelect
                    name={`${field}`}
                    control={control}
                    label={"Value"}
                    options={fieldOptions[field] || []}
                    rules={{ required: "Required." }}
                    onChange={(e, fn) => handleGroupFilterChange(e, fn, field)}
                  />
                )}
                {!(field in fieldOptions) && (
                  <FormInputText
                    name={`${field}`}
                    control={control}
                    label={"Value"}
                    options={fieldOptions[field] || []}
                    rules={{ required: "Required." }}
                    onChange={(e, fn) => handleGroupFilterChange(e, fn, field)}
                  />
                )}
              </Box>
            ))}
          </Grid>
          <Grid item xs={12} md={7}>
            {data && Object.keys(data).length > 0 && (
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
                  {Object.keys(geoJSON.features).length > 0 && (
                    <StationMap stations={geoJSON} metricName={displayMetric} />
                  )}
                </TabPanel>
                <TabPanel value="2">
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
