#!/usr/bin/env node
'use strict';

var GoogleSpreadsheet = require('google-spreadsheet');
var Sheet = new GoogleSpreadsheet('1LI4OBEGMPKxBwtGY_xJ1yT_G-rrCJxl8FaC0q0DaBl0');

var Firebase = require('firebase');

const TS_PATH = './node_modules/jstrueskill/lib/racingjellyfish/jstrueskill';
var TrueSkillCalculator = require(`${TS_PATH}/TrueSkillCalculator`);
var GameInfo = require(`${TS_PATH}/GameInfo`).getDefaultGameInfo();
var Player = require(`${TS_PATH}/Player`);
var Team = require(`${TS_PATH}/Team`);

// spreadsheet key is the long id in the sheets URL

function getSheetRows(sheet, tab) {
  return new Promise((resolve,reject) => {
    Sheet.getRows(tab, (err, rows) => {
      if (err !== null) {
        reject(err);
      }
      resolve(rows);
    });
  });
}

//Participants
function getParticipantMap(rows) {
  let participantMap = new Map();
  for (let row of rows) {
    let participantId = Number.parseInt(row.participantid);
    participantMap.set(participantId, {
      fullname: `${row.last}, ${row.first} (${row.nickname})`,
      name: {
        lastname: row.last,
        firstname: row.first,
        nickname: row.nickname,
      },
    });
  }
  return participantMap;
}

//Results
function getResults(rows) {
  let resultSet = new Set();
  for (let row of rows) {
    resultSet.add({
      raceId: row.raceid,
      leg: Number.parseInt(row.leg),
      driverId: Number.parseInt(row.driverid),
      navigatorId: 'n/a',
      startTime: Number.parseInt(row.startts),
      endTime: Number.parseInt(row.finishts),
      legTime: Number.parseInt(row.legts),
      dnf: row.dnf === 'TRUE' ? true : false,
      unknownTime: row.unknown === 'TRUE' ? true : false
    });
  }
  return Array.from(resultSet.values());
}

//Races
function getRaceMap(rows) {
  let raceMap = new Map();
  for (let row of rows) {
    raceMap.set(row.raceid, {
      raceId: row.raceid,
      name: row.racename,
      abbreviation: row.raceabbr,
      date: Number.parseInt(row.racets),
      numLegs: Number.parseInt(row.numlegs),
      legs: [],
      legMedianTimes: [],
      toScore: row.toscore === 'TRUE' ? true : false
    });
  }

  console.log(JSON.stringify(Array.from(raceMap), null, '\t'));

  return raceMap;
}

function buildRaceTables(participantMap, raceMap, results) {

  let raceTables = new Map();

  for (let race of raceMap.values()) {
    let table = results.filter(result => {
      return result.raceId === race.raceId;
    }).sort((res1, res2) => {
      if (res1.driverId !== res2.driverId) {
        return res1.driverId - res2.driverId;
      } else { return res1.leg - res2.leg; }
    }).filter(result => {
      return result.leg === 1;
    }).map(result => {
      return {
        driverId: result.driverId,
        driverName: participantMap.get(result.driverId).fullname,
        navigatorName: 'tbd',
        // leg1Start: new Date(raceDate.getTime() + result.startTime * 1000),
        // leg1End: new Date(raceDate.getTime() + result.endTime * 1000),
        // leg1Time: new Date(raceDate.getTime() + result.legTime * 1000),
        // leg1Rank: index + 1
      };
    });
    raceTables.set(race.raceId, table);
  }

  return raceTables;
}

function scoreLeg(results, participantMap) {
  results = results.filter(result => result.unknownTime === false)
  .sort((r1, r2) => {
    let t1 = r1.dnf ? Number.MAX_VALUE : r1.legTime;
    let t2 = r2.dnf ? Number.MAX_VALUE : r2.legTime;
    return t1 - t2;
  });

  //create teams
  let teams = results.map(result => {
    let driver = new Player(result.driverId);
    let driverSkill = participantMap.get(result.driverId).driverSkill;
    if (typeof driverSkill === 'undefined') {driverSkill = GameInfo.getDefaultRating();}
    return new Team(result.driverId.toString(), driver, driverSkill);
  });

  //create rankings`
  //TODO: change to a map
  let finishOrder = [];
  results.forEach((result, index) => {
    finishOrder.push(result.dnf ? results.length : index + 1);
  });

  let newSkills = TrueSkillCalculator.calculateNewRatings(GameInfo, teams, finishOrder);

  //update scores
  teams.forEach(team => {
    let player = team.getPlayers()[0];
    participantMap.get(player.getId()).driverSkill = newSkills[player];
  });

}

console.log('Getting data from Google Sheets');
Promise.all([getSheetRows(Sheet, 1).then(getParticipantMap),
             getSheetRows(Sheet, 3).then(getRaceMap),
             getSheetRows(Sheet, 2).then(getResults)
           ])
.then((promiseResults) => {
  console.log('Data from sheets pulled');
  let participantMap = promiseResults[0];
  let raceMap = promiseResults[1];
  let results = promiseResults[2];

  //Iterate throught he races
  for (let race of raceMap.values()) {
    if (race.toScore === true) {
      for (let leg = 1; leg <= race.numLegs; leg++) { //using human numbers
        let legResults = results.filter(result => {
          return (result.raceId === race.raceId) && (result.leg === leg);
        });
        if (legResults.length > 0) {
          console.log(`scoring ${race.raceId}: ${leg}`);
          scoreLeg(legResults, participantMap);

          raceMap.get(race.raceId).legs[leg] = legResults;

          //calculate median leg times
          let sortedLegTimes = legResults.filter(leg => {
            return leg.dnf === false && leg.unknownTime === false;
          })
          .map(leg => {return leg.legTime;})
          .sort((a, b) => {return a - b;});
          let medianLegTime = (sortedLegTimes[Math.floor(sortedLegTimes.length / 2)] +
                               sortedLegTimes[Math.ceil(sortedLegTimes.length / 2)]) / 2;
          raceMap.get(race.raceId).legMedianTimes[leg] = medianLegTime;

        }
      }
    }
  }

  console.log('Updating Firebase');
  let fbRef = new Firebase('https://dttdata.firebaseio.com/');
  fbRef.set({
    raceMap: Array.from(raceMap),
    results: results,
    participantMap: Array.from(participantMap),
    raceTables: Array.from(buildRaceTables(participantMap, raceMap, results))
  });

  //output the ranked list of drivers
  let rankedDrivers = Array.from(participantMap.values())
  .filter(a => { return typeof a.driverSkill !== 'undefined';})
  .sort((a,b) => {
    return b.driverSkill.conservativeRating - a.driverSkill.conservativeRating;
  });
  rankedDrivers.forEach((driver,index) => {
    console.log(`${index} ${driver.driverSkill.conservativeRating.toFixed(3)} ` +
                `${driver.driverSkill.mean.toFixed(3)} ` +
                `${driver.name.lastname}, ${driver.name.firstname}`);
  });

  console.log('all done');
})
.catch(err => {
  console.error(`ERROR: ${err}`);
  console.error(err.stack);
  throw err;
});
