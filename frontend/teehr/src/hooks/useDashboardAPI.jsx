import { useState } from "react";
import axios from "axios";

const useDashboardAPI = () => {
  const baseURL = "http://localhost:8000";
  const [loading, setLoading] = useState(false);
  const [errors, setErrors] = useState(false);

  const fetchDatasets = () => {
    setLoading(true);
    return axios
      .get(`${baseURL}/datasets/`)
      .then((res) => {
        setLoading(false);
        return Object.keys(res.data);
      })
      .catch((err) => {
        setErrors(err);
        setLoading(false);
      });
  };

  const fetchMetricFields = (dataset) => {
    setLoading(true);
    return axios
      .get(`${baseURL}/datasets/${dataset}/get_metric_fields`)
      .then((res) => {
        setLoading(false);
        return res.data;
      })
      .catch((err) => {
        setErrors(err);
        setLoading(false);
      });
  };

  const fetchGroupByFields = (dataset) => {
    setLoading(true);
    return axios
      .get(`${baseURL}/datasets/${dataset}/get_data_fields`)
      .then((res) => {
        setLoading(false);
        return res.data;
      })
      .catch(function (err) {
        setErrors(err);
        setLoading(false);
      });
  };

  const fetchOptionsForField = (field, dataset) => {
    setLoading(true);
    return axios
      .post(`${baseURL}/datasets/${dataset}/get_unique_field_values`, {
        field_name: field,
      })
      .then((res) => {
        const options = res.data.map((o) => Object.values(o)[0]);
        setLoading(false);
        return options;
      })
      .catch(function (err) {
        setErrors(err);
        setLoading(false);
      });
  };

  const fetchOptionsForMultipleFields = async (fields, dataset) => {
    setLoading(true);
    const fieldValues = {};
    await Promise.all(
      fields.map(async (field) => {
        try {
          const res = await axios.post(
            `${baseURL}/datasets/${dataset}/get_unique_field_values`,
            {
              field_name: field,
            }
          );
          const options = res.data.map((o) => Object.values(o)[0]);
          fieldValues[field] = options;
        } catch (err) {
          setLoading(false);
          setErrors(err);
        }
      })
    );
    setLoading(false);
    return fieldValues;
  };

  const fetchFilterOperators = (dataset) => {
    setLoading(true);
    return axios
      .get(`${baseURL}/datasets/${dataset}/get_filter_operators`)
      .then((res) => {
        setLoading(false);
        return res.data;
      })
      .catch(function (err) {
        setErrors(err);
        setLoading(false);
      });
  };

  const fetchStations = (dataset, groupFields, metrics, filters) => {
    setLoading(true);
    return axios
      .post(`${baseURL}/datasets/${dataset}/get_metrics`, {
        group_by: groupFields,
        order_by: ["primary_location_id"],
        include_metrics: metrics,
        filters: filters,
        return_query: false,
        include_geometry: true,
      })
      .then((res) => {
        setLoading(false);
        return res;
      })
      .catch(function (err) {
        setErrors(err);
        setLoading(false);
      });
  };

  return {
    loading,
    errors,
    fetchDatasets,
    fetchMetricFields,
    fetchGroupByFields,
    fetchOptionsForField,
    fetchOptionsForMultipleFields,
    fetchFilterOperators,
    fetchStations,
  };
};

export default useDashboardAPI;
