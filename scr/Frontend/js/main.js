// --- main.js ---
// Este script controla el mapa y la lógica de carga de proyectos.

let map = L.map('map').setView([40.4168, -6.7038], 6);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

let currentLayer = null;
let currentCommunitiesLayer = null; // Variable para almacenar la capa de comunidades

// Función que carga un proyecto específico según su nombre
function loadProject(projectName) {
  if (currentLayer) {
    map.removeLayer(currentLayer);
    currentCommunitiesLayer = null;
  }

  if (projectName === "Proyecto_aguas") {
    currentLayer = loadProyecto1(map);
    // Asume que la capa de comunidades se retorna o se puede acceder
    currentCommunitiesLayer = currentLayer;
  } else if (projectName === "Proyecto_rubros") {
    currentLayer = loadProyecto2(map);
  }
}

// Evento que detecta cuando el usuario cambia el proyecto desde el menú
document.getElementById("projectSelector").addEventListener("change", function() {
  loadProject(this.value);
});

// Carga inicial (Por defecto el primero)
loadProject("Proyecto_aguas");


// --- Nueva función para añadir el control de las Islas Canarias ---
function addCanaryIslandsControl(mainMap) {
  // Crea el control de Leaflet
  const canaryControl = L.Control.extend({
    onAdd: function(map) {
      // Crea el contenedor del mini-mapa
      const container = L.DomUtil.create('div', 'canary-islands-control');
      // Crea el mini-mapa de Leaflet dentro del contenedor
      const miniMap = L.map(container, {
        zoomControl: false,
        attributionControl: false,
        dragging: false,
        scrollWheelZoom: false,
        boxZoom: false
      }).setView([30.5, -20.3], 5); // Centrado en las Canarias con zoom 6

      // Añade la capa base al mini-mapa
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(miniMap);

      // Carga el GeoJSON de las comunidades
      fetch('scr/Backend/data/Input/comunidades_autonomas.geojson')
        .then(res => res.json())
        .then(data => {
          // Filtra solo la capa de Canarias
          const canariasFeature = data.features.find(f => f.properties.name === "Canarias");
          if (canariasFeature) {
            const canariasLayer = L.geoJSON(canariasFeature, {
              style: {
                color: 'blue',
                weight: 2,
                fillOpacity: 0.5
              },
              onEachFeature: function(feature, layer) {
                // Al hacer clic en el mini-mapa, se activa la lógica en el mapa principal
                layer.on('click', function() {
                  const communitiesLayer = map.eachLayer(function(l) {
                    if (l.toGeoJSON && l.toGeoJSON().features && l.toGeoJSON().features[0].properties.name === 'Canarias') {
                      l.fire('click');
                    }
                  });
                });
              }
            }).addTo(miniMap);
          }
        });

      return container;
    },
    onRemove: function(map) {}
  });

  const control = new canaryControl({ position: 'bottomleft' }).addTo(mainMap);
  const controlDiv = control.getContainer();

  // Oculta el control inicialmente
  controlDiv.style.display = 'none';

  // Muestra u oculta el control según el nivel de zoom
  mainMap.on('zoomend', function() {
    if (mainMap.getZoom() >= 6) {
      controlDiv.style.display = 'block';
    } else {
      controlDiv.style.display = 'none';
    }
  });
}

// Llama a la nueva función para añadir el control de Canarias
addCanaryIslandsControl(map);