const Influx = require('influx');
const _influx = function(node) {
  if(typeof node.config == 'undefined') {
    node.config = node;
  }
  return new Influx.InfluxDB({
   host: node.config.influxhost,
   database: node.config.influxdb,
   port:node.config.influxport,
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
          console.log(result);
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
    return new Promise(async function (resolve, reject)  {
      if(typeof node.config == 'undefined') node.config = node;
      let scaleFactor = 1000*10000000;
      if(typeof node.config.scaleFactor !== 'undefined') scaleFactor = node.config.scaleFactor;

      let measurement = node.config.influx_feedin_measurement;
      let field = 'energy';

      console.log('InfluxDB meterId',meterId);

      const getMeasurement = function(field,measutement) {
        return new Promise(function (resolve2, reject2)  {
        _influx(node).query('SELECT first("'+field+'") as firstEnergy,last("'+field+'") as lastEnergy FROM "'+measurement+'" WHERE time>\''+new Date(from).toISOString()+'\'  AND  time<\''+new Date(to).toISOString()+'\' LIMIT 1000').then(result =>
            {
              resolve2(result);
            });
        });
      }

      if(meterId == 'influx-feedin') {
          measurement = node.config.influx_feedin_measurement;
          field = node.config.influx_feedin_field;
      }
      if(meterId == 'influx-feedout') {
        measurement = node.config.influx_feedout_measurement;
        field = node.config.influx_feedout_field;
      }
      if(meterId == 'influx-prod') {
        measurement = node.config.influx_prod_measurement;
        field = node.config.influx_prod_field;
      }

      if((measurement.length > 0) && (field.length >0)) {
          let result = await getMeasurement(field,measurement);
          // patch resultOut
          let resultOut = await getMeasurement(node.config.influx_feedout_field,node.config.influx_feedout_measurement);
          let responds = [];
          let firstOut = 0;
          let lastOut = 0;
          if(resultOut.length >0) {
            firstOut = resultOut[0].firstEnergy;
            lastOut = resultOut[0].lastEnergy;
          }
          responds.push({
              time: from,
              values: {
                energy: Math.round(result[0].firstEnergy * scaleFactor),
                energyOut: Math.round(firstOut * scaleFactor)
              }
          });
          responds.push({
              time: to,
              values: {
                energy: Math.round(result[0].lastEnergy * scaleFactor),
                energyOut: Math.round(lastOut * scaleFactor)
              }
          });
          resolve(responds);
      } else {
            resolve([]);
      }
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
