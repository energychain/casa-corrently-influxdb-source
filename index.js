const Influx = require('influx');
const _influx = function(node) {
  if(typeof node.config == 'undefined') {
    node.config = node;
  }
  return new Influx.InfluxDB({
   host: node.config.influxhost,
   database: node.config.influxdb,
   schema: [
     {
       measurement: node.influxmeasurement,
       fields: {
         total_consumption: Influx.FieldType.INTEGER,
         total_saving: Influx.FieldType.INTEGER
       },
       tags: [
         'corrently-meter'
       ]
     }
   ]
 });
};

module.exports = {
  last_reading: function(meterId,node) {
      return new Promise(function (resolve, reject)  {
        let fieldin = node.config.influx_feedin_field;
        let fieldout = node.config.influx_feedout_field;
        let measurement = node.config.influx_feedin_measurement;

        if(meterId == 'influx-prod') {
          fieldin = node.config.influx_prod_field;
          delete fieldout;
          measurement = node.config.influx_prod_measurement;
        }

        let scaleFactor = 1000*10000000;
        if((typeof node.config !== 'undefined') && (typeof node.config.scaleFactor !== 'undefined')) scaleFactor = node.config.scaleFactor;
        _influx(node).query('SELECT last("'+fieldin+'") as energy,last("'+fieldout+'") as energyOut  FROM "'+measurement+'" WHERE time>now()-1d LIMIT 1000').then(result => {
          if(result.length < 1 ) {
              reject("No Measurement");
          } else {
            let responds = {
                time: new Date(result[0].time).getTime(),
                values: {
                  energy: Math.round(result[0].energy * scaleFactor),
                  energyOut: Math.round(result[0].energyOut * scaleFactor)
                }
            };
            resolve(responds);
          }

        }).catch(err => {
          reject(err.stack);
        });
    });
  },
  historicReading: async function(meterId,resolution,from,to,node) {
    return new Promise(function (resolve, reject)  {
      if(typeof node.config == 'undefined') node.config = node;
      let scaleFactor = 1000*10000000;
      let fieldin = node.config.influx_feedin_field;
      let fieldout = node.config.influx_feedout_field;
      let measurement = node.config.influx_feedin_measurement;

      if((typeof node.config !== 'undefined') && (typeof node.config.scaleFactor !== 'undefined')) scaleFactor = node.config.scaleFactor;
      _influx(node).query("SELECT first("+fieldin+") as firstEnergy,last("+fieldin+") as lastEnergy FROM "+measurement+" WHERE time>'"+new Date(from).toISOString()+"'  AND  time<'"+new Date(to).toISOString()+"' LIMIT 1000").then(result => {
        if(result.length < 1 ) {
            resolve([]);
        } else {

          let responds = [];
          responds.push({
              time: from,
              values: {
                energy: Math.round(result[0].firstEnergy * scaleFactor),
                energyOut: Math.round(0 * scaleFactor)
              }
          });
          responds.push({
              time: to,
              values: {
                energy: Math.round(result[0].lastEnergy * scaleFactor),
                energyOut: Math.round(0 * scaleFactor)
              }
          });
          if(typeof fieldout !== 'undefined') {
            _influx(node).query("SELECT first("+fieldout+") as firstEnergy,last("+fieldout+") as lastEnergy FROM "+measurement+" WHERE time>'"+new Date(from).toISOString()+"'  AND  time<'"+new Date(to).toISOString()+"' LIMIT 1000").then(result => {
              if(result.length < 1 ) {
                  resolve(responds);
              } else {
                  responds[0].values.energyOut = Math.round(result[0].firstEnergy * scaleFactor);
                  responds[1].values.energyOut = Math.round(result[0].lastEnergy * scaleFactor);
                  resolve(responds);
              }
            });
          } else resolve(responds);
        }
      }).catch(err => {
        reject(err.stack);
      });
    });
  },
  meters: async function(node) {
    let responds = [];
    responds.push({
      meterId:'influx-feedin',
      firstMeasurementTime:0,
      location: {
        country: 'DE',
        zip: node.config.zip
      }
    });
    responds.push({
      meterId:'influx-feedout',
      firstMeasurementTime:0,
      location: {
        country: 'DE',
        zip: node.config.zip
      }
    });
    responds.push({
      meterId:'influx-prod',
      firstMeasurementTime:0,
      location: {
        country: 'DE',
        zip: node.config.zip
      }
    });
    return responds;
  }
};
