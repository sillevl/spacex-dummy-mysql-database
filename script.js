const capsules = require('./json/capsules.json');
const cores = require('./json/cores.json');
const launches = require('./json/launches.json');
const launchpads = require('./json/launchpads.json');
const missions = require('./json/missions.json');
const payloads = require('./json/payloads.json');
const rockets = require('./json/rockets.json');

var jsonSql = new (require('json-sql').Builder)({separatedValues: false, dialect: 'mysql', wrappedIdentifiers: false});

let data = [];

const fs = require('fs');


// helper functions

function findCapsule(flight_number) {
  result = null;
  capsules.forEach(capsule => {
    capsule.missions.forEach(mission => {
      if(mission.flight == flight_number) {
        result = capsule.capsule_id;
      }
    })
  })
  return result;
}

function findMission(mission_id) {
  result = null;
  missions.forEach(mission => {
    mission.payload_ids.forEach(payload => {
      if(payload == mission_id) {
        result = mission.mission_id;
      }
    })
  })
  return result;
}

function findCore(flight_number) {
  result = null;
  cores.forEach(core => {
    core.missions.forEach(mission => {
      if(mission.flight == flight_number) {
        result = core.core_serial;
      }
    })
  })
  return result;
}

function findLaunch(payload_id) {
  result = null;
  launches.forEach(launch => {
    launch.rocket.second_stage.payloads.forEach(payload => {
      if(payload.payload_id == payload_id) {
        result = launch.flight_number;
      }
    })
  })
  return result;
}

console.log(findLaunch("CRS-7"))


// normalizing datasets

data.launchpads = launchpads.map(launchpad => {
  return {
    id: launchpad.site_id,
    name: launchpad.site_name_long,
    status: launchpad.status,
    location: launchpad.location.name,
    region: launchpad.location.region,
    details: launchpad.details
  }
})

data.cores = cores.map(core => {
  return {
    serial: core.core_serial,
    block: core.block,
    status: core.status,
    details: core.details,
    water_landing: core.water_landing
  }
})

data.capsules = capsules.map(capsule => {
  return {
    serial: capsule.capsule_serial,
    type: capsule.type,
    status: capsule.status,
    details: capsule.details
  }
})

data.rockets = rockets.map(rocket => {
  return {
    id: rocket.id,
    active: rocket.active,
    stages: rocket.stages,
    booster: rocket.booster,
    cost_per_launch: rocket.cost_per_launch,
    company: rocket.company,
    height: rocket.height.meters,
    diameter: rocket.diameter.meters,
    mass: rocket.mass.kg,
    name: rocket.rocket_name,
    description: rocket.description,
    engines: rocket.engines.type
  }
})

data.missions = missions.map(mission => {
  return {
    id: mission.mission_id,
    name: mission.mission_name,
    manufacturers: mission.manufacturers.join(),
    description: mission.description,
  }
})

data.payloads = payloads.map(payload => {
  return {
    id: payload.payload_id,
    reused: payload.reused,
    manufacturer: payload.manufacturer,
    type: payload.payload_type,
    mass: payload.payload_mass_kg,
    orbit: payload.orbit,
    nationality: payload.nationality,
    customers: payload.customers.join(),
    mission_id: findMission(payload.payload_id),
    launch_id: findLaunch(payload.payload_id)
  }
})

data.launches = launches.map(launch => {
  return {
    id: launch.flight_number,
    flight_number: launch.flight_number,
    mission_name: launch.mission_name,
    upcoming: launch.upcoming,
    year: launch.launch_year,
    timestamp: launch.launch_date_unix,
    is_tentative: launch.is_tentative,
    tbd: launch.tbd,
    rocket_id: launch.rocket.rocket_id,
    launchpad_id: launch.launch_site.site_id,
    details: launch.details,
    success: launch.launch_success,
    capsule_serial: findCapsule(launch.flight_number),
    core_serial: findCore(launch.flight_number)
  }
})


Object.keys(data).forEach(entity => {
  console.log("Entity: " + entity)
  const filename = `sql/${entity}.sql`;
  try {
    fs.unlinkSync(filename);
  } catch (err) {
  }
  data[entity].forEach(row => {
    Object.keys(row).forEach(field => {
      if(typeof(row[field]) === "string") {
        row[field] = row[field].replace(/'/g, '&#39;');
      }
    })
    var sql = jsonSql.build({
      type: 'insert',
      table: entity,
      values: row
    });
    //console.log(sql.query)
    fs.appendFileSync(filename, `${sql.query}\r\n`);
  })
})
