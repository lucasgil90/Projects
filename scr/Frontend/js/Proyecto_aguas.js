  // URLs de los archivos GeoJSON.

// --- Proyecto_aguas.js ---

function loadProyecto1(map) {
  let layerGroup = L.layerGroup();
  let comunidadesLayer = null;
  let puntosDeAgua = null;
  let visiblePuntosLayer = null;

  const geojsonComunidadesUrl  = 'Backend/data/Input/comunidades_autonomas.geojson';
  const geojsonAbastecimientosUrl = 'Backend/data/Output/abastecimientos_test.geojson';

  // --- Lógica de carga asíncrona con Promise.all ---
  Promise.all([
    fetch(geojsonComunidadesUrl).then(response => response.json()),
    fetch(geojsonAbastecimientosUrl).then(response => response.json())
  ])
  .then(([comunidadesData, abastecimientosData]) => {
    puntosDeAgua = L.geoJSON(abastecimientosData, {
      pointToLayer: (feature, latlng) => {
        let calidad = feature.properties["Calidad del agua"];
        let colorRelleno;
        if (calidad === "Agua apta para el consumo") {
          colorRelleno = 'green';
        } else if (calidad === "Agua no apta para el consumo") {
          colorRelleno = 'red';
        } else {
          colorRelleno = 'gray';
        }
        let popupContent = Object.entries(feature.properties).map(([key, value]) => `<b>${key}:</b> ${value}`).join('<br>');
        return L.circleMarker(latlng, {
          radius: 6,
          color: 'black',
          weight: 1,
          fillColor: colorRelleno,
          fillOpacity: 0.8
        }).bindPopup(popupContent);
      }
    });

    comunidadesLayer = L.geoJSON(comunidadesData, {
      style: function(feature) {
        const randomColor = '#' + Math.floor(Math.random() * 16777215).toString(16).padStart(6, '0');
        return {
          color: 'black',
          weight: 1,
          opacity: 0.8,
          fillColor: randomColor,
          fillOpacity: 0.4
        };
      },
      onEachFeature: function(feature, layer) {
        layer.on('click', function(e) {
          L.DomEvent.stopPropagation(e); // Detiene la propagación del clic para que no active el evento del mapa
          
          if (visiblePuntosLayer) {
            map.removeLayer(visiblePuntosLayer);
          }

          comunidadesLayer.eachLayer(function(l) {
            comunidadesLayer.resetStyle(l);
            l.setStyle({
              fillColor: '#808080',
              color: '#404040'
            });
          });
          layer.setStyle({
            weight: 3,
            color: 'blue',
            fillOpacity: 0.7,
            fillColor: '#2074e8'
          });
          
          map.fitBounds(e.target.getBounds(), {
              padding: L.point(20, 20),
              easeLinearity: 0.5,
              maxZoom: 10
          });

          const puntosEnComunidad = puntosDeAgua.toGeoJSON().features.filter(
            punto => turf.booleanPointInPolygon(punto, feature)
          );
          
          visiblePuntosLayer = L.geoJSON(puntosEnComunidad, {
            pointToLayer: (feature, latlng) => {
              let calidad = feature.properties["Calidad del agua"];
              let colorRelleno;
              if (calidad === "Agua apta para el consumo") {
                colorRelleno = 'green';
              } else if (calidad === "Agua no apta para el consumo") {
                colorRelleno = 'red';
              } else {
                colorRelleno = 'gray';
              }
              let popupContent = Object.entries(feature.properties).map(([key, value]) => `<b>${key}:</b> ${value}`).join('<br>');
              return L.circleMarker(latlng, {
                radius: 6,
                color: 'black',
                weight: 1,
                fillColor: colorRelleno,
                fillOpacity: 0.8
              }).bindPopup(popupContent);
            }
          });
          visiblePuntosLayer.addTo(map);

          const centroide = turf.centroid(feature).geometry.coordinates;
          const latlngCentroide = L.latLng(centroide[1], centroide[0]);

          const popupContent = createPopupContent(visiblePuntosLayer, feature);
          L.popup({
              closeButton: false,
              className: 'stats-popup'
            })
            .setLatLng(latlngCentroide)
            .setContent(popupContent)
            .openOn(map);
        });
      }
    }).addTo(layerGroup);

    // --- NUEVA LÓGICA: Restaurar la vista al hacer clic en el mapa ---
    map.on('click', function(e) {
      // Oculta los puntos si están visibles
      if (visiblePuntosLayer) {
        map.removeLayer(visiblePuntosLayer);
      }
      // Restablece el estilo original de todas las comunidades
      comunidadesLayer.eachLayer(function(l) {
        comunidadesLayer.resetStyle(l);
      });
      // Cierra cualquier pop-up abierto
      map.closePopup();
      // Vuelve a la vista inicial del mapa
      map.setView([40.4168, -6.7038], 6);
    });
  })
  .catch(error => {
    console.error("Error al cargar los datos:", error);
    alert("Hubo un error al cargar los datos del mapa. Revisa la consola para más detalles.");
  });

  // --- Lógica del Pop-up y Porcentajes ---
  function createPopupContent(puntos, comunidadGeoJSON) {
    if (!puntos) return "No hay datos de abastecimientos disponibles.";

    let totalAbastecimientos = 0;
    let aptos = 0;
    let noAptos = 0;
    puntos.eachLayer(function(punto) {
      totalAbastecimientos++;
      if (punto.feature.properties["Calidad del agua"] === "Agua apta para el consumo") {
        aptos++;
      } else if (punto.feature.properties["Calidad del agua"] === "Agua no apta para el consumo") {
        noAptos++;
      }
    });

    const porcentajeAptos = totalAbastecimientos > 0 ? ((aptos / totalAbastecimientos) * 100).toFixed(2) : 0;
    const porcentajeNoAptos = totalAbastecimientos > 0 ? ((noAptos / totalAbastecimientos) * 100).toFixed(2) : 0;

    return `
      <div style="font-family: Arial, sans-serif; text-align: center;">
        <h4 style="margin: 0; font-size: 1.2em;">Abastecimientos</h4>
        <hr style="border-top: 1px solid #ccc; margin: 5px 0;">
        <p style="margin: 5px 0;">Total Abastecimientos: <b>${totalAbastecimientos}</b></p>
        <p style="margin: 5px 0; color: green;">Aptos: <b>${aptos}</b> (${porcentajeAptos}%)</p>
        <p style="margin: 5px 0; color: red;">No Aptos: <b>${noAptos}</b> (${porcentajeNoAptos}%)</p>
      </div>
    `;
  }

  layerGroup.addTo(map);
  return layerGroup;
}