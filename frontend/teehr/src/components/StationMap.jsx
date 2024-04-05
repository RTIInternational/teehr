import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import "../App.css";
import { MapContainer, TileLayer, GeoJSON } from "react-leaflet";
import L from "leaflet";
import colormap from "colormap";

function StationMap(props) {
  const position = [39, -98];
  const { stations, metricName } = props;
  const [map, setMap] = useState(null);
  const [metricMin, setMetricMin] = useState(null);
  const [metricMax, setMetricMax] = useState(null);

  //set style
  const steps = 50;
  const ramp = colormap({
    colormap: "jet",
    nshades: steps,
  });
  const clamp = (value, low, high) => {
    return Math.max(low, Math.min(value, high));
  };
  const getColor = (feature) => {
    const value = feature.properties[metricName];
    const f = Math.pow(
      clamp((value - metricMin) / (metricMax - metricMin), 0, 1),
      1 / 2
    );
    const index = Math.round(f * (steps - 1));
    return ramp[index];
  };

  useEffect(() => {
    const setMinMax = (stations, metricName) => {
      if (stations) {
        const values = stations.features.map(
          (feature) => feature.properties[metricName]
        );
        setMetricMin(Math.min(...values));
        setMetricMax(Math.max(...values));
      }
    };
    setMinMax(stations, metricName);
  }, [stations, metricName]);

  useEffect(() => {
    if (map && stations) {
      const layer = L.geoJSON(stations);
      map.fitBounds(layer.getBounds());
    }
  }, [map, stations]);

  const onEachFeature = (feature, layer) => {
    layer.on({
      // click: () => {
      //   // console.log("clicked: " + feature.properties.name + " - " + feature.id)
      //   geoJsonRef.current.getLayers().forEach((layer2) => {
      //     layer2
      //       .getLayers()
      //       .forEach((marker) => marker.setIcon(stationIcon(layer2.feature)));
      //   });
      //   layer.getLayers().forEach((marker) => {
      //     marker.setIcon(selectedStationIcon(feature));
      //   });
      //   setSelectedFeature(feature);
      // },
      mouseover: () => {
        layer.bindTooltip(
          feature.properties.primary_location_id +
            " (" +
            feature.properties[metricName].toFixed(3) +
            ")",
          { direction: "right" }
        );
        layer.openTooltip();
      },
      mouseout: () => {
        layer.closeTooltip();
      },
    });
  };

  const StationGeoJSON = () => {
    return stations ? (
      <GeoJSON
        key={stations}
        data={stations}
        pointToLayer={(feature, latlng) => {
          return new L.CircleMarker(latlng, {
            radius: 5,
            color: getColor(feature),
            fillOpacity: 1,
          });
        }}
        onEachFeature={onEachFeature}
      />
    ) : null;
  };

  return (
    <MapContainer
      center={position}
      zoom={4}
      scrollWheelZoom={true}
      ref={setMap}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <StationGeoJSON />
    </MapContainer>
  );
}

StationMap.propTypes = {
  stations: PropTypes.object.isRequired,
  metricName: PropTypes.string.isRequired,
};

export default StationMap;
