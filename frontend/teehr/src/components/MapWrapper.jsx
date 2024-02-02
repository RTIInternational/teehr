// react
import { useState, useEffect, useRef } from 'react';

// openlayers
import Map from 'ol/Map';
import View from 'ol/View';
import TileLayer from 'ol/layer/Tile';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import XYZ from 'ol/source/XYZ';
import GeoJSON from 'ol/format/GeoJSON.js';
// import { transform } from 'ol/proj';
import { toStringXY } from 'ol/coordinate';
import { Circle as CircleStyle, Fill, Stroke, Style, Text } from 'ol/style.js';
import colormap from 'colormap';

function MapWrapper(props) {

    // set intial state
    const { stations, metricName} = props;
    const [map, setMap] = useState()
    const [featuresLayer, setFeaturesLayer] = useState()
    const [selectedCoord, setSelectedCoord] = useState()


    // pull refs
    const mapElement = useRef()

    // create state ref that can be accessed in OpenLayers onclick callback function
    //  https://stackoverflow.com/a/60643670
    const mapRef = useRef()
    mapRef.current = map


    // initialize map on first render - logic formerly put into componentDidMount
    useEffect(() => {

        // create and add vector source layer
        const initalFeaturesLayer = new VectorLayer({
            source: new VectorSource(),
        })

        // create map
        const initialMap = new Map({
            target: mapElement.current,
            layers: [

                // USGS Topo
                new TileLayer({
                    source: new XYZ({
                        url: 'https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer/tile/{z}/{y}/{x}',
                    })
                }),

                // Google Maps Terrain
                /* new TileLayer({
                  source: new XYZ({
                    url: 'http://mt0.google.com/vt/lyrs=p&hl=en&x={x}&y={y}&z={z}',
                  })
                }), */

                initalFeaturesLayer

            ],
            view: new View({
                // projection: 'EPSG:3857',
                projection: 'EPSG:4326',
                center: [0, 0],
                zoom: 2
            }),
            controls: []
        })

        // set map onclick handler
        initialMap.on('click', handleMapClick)

        // save map and vector layer references to state
        setMap(initialMap)
        setFeaturesLayer(initalFeaturesLayer)

        return () => initialMap.setTarget(null)

    }, [])

    // update map if features prop changes - logic formerly put into componentDidUpdate
    useEffect(() => {

        if (stations) { // may be null on first render

            const feats = new GeoJSON().readFeatures(stations)
            const values = feats.map((feat) => feat.get(metricName))
            const min = Math.min(...values)
            const max = Math.max(...values)

            const src = new VectorSource({
                features: feats
            })

            // set features to map
            featuresLayer.setSource(src)

            //set style
            const steps = 50;
            const ramp = colormap({
                colormap: 'jet',
                nshades: steps,
            });
            const clamp = (value, low, high) => {
                return Math.max(low, Math.min(value, high));
            }

            const getColor = (feature) => {
                const value = feature.get(metricName);
                const f = Math.pow(clamp((value - min) / (max - min), 0, 1), 1 / 2);
                const index = Math.round(f * (steps - 1));
                return ramp[index];
            }

            const getMetricValue = (feature) => {
                return feature.get(metricName).toFixed(4).toString();
            }

            const styleFunction = (feature) => {
                return new Style({
                    image: new CircleStyle({
                        radius: 5,
                        fill: new Fill({
                            color: getColor(feature),
                        }),
                        stroke: new Stroke({
                            color: 'rgba(0,0,0,0.5)',
                        }),
                    }),
                    text: new Text({
                        text: getMetricValue(feature),
                        textAlign: "left",
                        fill: new Fill({
                            color: 'rgba(0, 0, 0, 0.6)',
                          }),
                          stroke: new Stroke({
                            color: 'rgba(255, 255, 255, 0.6)',
                            width: 10,
                          }),
                        offsetX: 15,
                    }),
                });
            }
            featuresLayer.setStyle(styleFunction)

            // fit map to feature extent (with 100px of padding)
            map.getView().fit(featuresLayer.getSource().getExtent(), {
                padding: [100, 100, 100, 100]
            })

        }

    }, [featuresLayer, map, metricName, stations])

    // map click handler
    const handleMapClick = (event) => {

        // get clicked coordinate using mapRef to access current React state inside OpenLayers callback
        //  https://stackoverflow.com/a/60643670
        const clickedCoord = mapRef.current.getCoordinateFromPixel(event.pixel);

        // transform coord to EPSG 4326 standard Lat Long
        // const transormedCoord = transform(clickedCoord, 'EPSG:3857', 'EPSG:4326')

        // set React state
        setSelectedCoord(clickedCoord)

    }

    // render component
    return (
        <div>

            <div ref={mapElement} className="map-container"></div>

            <div className="clicked-coord-label">
                <p>{(selectedCoord) ? toStringXY(selectedCoord, 5) : ''}</p>
            </div>

        </div>
    )

}

export default MapWrapper
