#!/usr/bin/env node
'use strict';

var GoogleSpreadsheet = require('google-spreadsheet');
var Firebase = require('firebase');

const TS_PATH = './node_modules/jstrueskill/lib/racingjellyfish/jstrueskill';
var TrueSkillCalculator = require(`${TS_PATH}/TrueSkillCalculator`);
var GameInfo = require(`${TS_PATH}/GameInfo`);
var Player = require(`${TS_PATH}/Player`);
var Team = require(`${TS_PATH}/Team`);

// spreadsheet key is the long id in the sheets URL
var sheet = new GoogleSpreadsheet('1LI4OBEGMPKxBwtGY_xJ1yT_G-rrCJxl8FaC0q0DaBl0');
var gameInfo = GameInfo.getDefaultGameInfo();

function getSheetRows(sheet, tab) {
  return new Promise((resolve,reject) => {
    sheet.getRows(tab, (err, rows) => {
      if (err !== null) {
        reject(err);
      }
      resolve(rows);
    });
  });
}

//Participants
var participantPromise = rows => {
  let participants = {};
  rows.forEach(row => {
    let participantId = parseInt(row.participantid);
    participants[participantId] = {
      participantId: participantId,
      name: {
        lastname: row.last,
        firstname: row.first,
        nickname: row.nickname,
      },
      races: {}
    };
  });
  return participants;
};

//Results
var resultsPromise = (rows) => {
  let results = [];
  rows.forEach(row => {
    results.push({
      raceId: row.raceid,
      leg: parseInt(row.leg),
      driverId: parseInt(row.driverid),
      navigatorId: 'n/a',
      startTime: parseInt(row.startts),
      endTime: parseInt(row.finishts),
      legTime: parseInt(row.legts),
      dnf: row.dnf === 'TRUE' ? true : false,
      unknownTime: row.unknown === 'TRUE' ? true : false
    });
  });
  return results;
};

//Races
var racePromise = (rows) => {
  let races = {};
  rows.forEach(row => {
    races[row.raceid] = {
      raceId: row.raceid,
      year: parseInt(row.year),
      name: row.racename,
      abbreviation: row.raceabbr,
      dateStr: row.date,
      date: parseInt(row.racets),
      numLegs: parseInt(row.numlegs),
      legs: [],
      legMedianTimes: [],
      toScore: row.toscore === 'TRUE' ? true : false
    };
  });
  return races;
};

function scoreLeg(results, participants) {
  results = results.filter(result => result.unknownTime === false)
  .sort((r1, r2) => {
    let t1 = r1.dnf ? Number.MAX_VALUE : r1.legTime;
    let t2 = r2.dnf ? Number.MAX_VALUE : r2.legTime;
    return t1 - t2;
  });

  //create teams
  let teams = results.map(result => {
    let driver = new Player(result.driverId);
    let driverSkill = participants[result.driverId].driverSkill;
    if (typeof driverSkill === 'undefined') {driverSkill = gameInfo.getDefaultRating();}
    return new Team(result.driverId.toString(), driver, driverSkill);
  });

  //create rankings`
  let finishOrder = [];
  results.forEach((result, index) => {
    finishOrder.push(result.dnf ? results.length : index + 1);
  });

  let newSkills = TrueSkillCalculator.calculateNewRatings(gameInfo, teams, finishOrder);

  //update scores
  teams.forEach(team => {
    let player = team.getPlayers()[0];
    participants[player.getId()].driverSkill = newSkills[player];
  });

}

console.log('Getting data from Google Sheets');
Promise.all([getSheetRows(sheet, 1).then(participantPromise),
             getSheetRows(sheet, 2).then(resultsPromise),
             getSheetRows(sheet, 3).then(racePromise)
           ])
.then((promiseResults) => {
  console.log('Data from sheets pulled');
  let participants = promiseResults[0];
  let results = promiseResults[1];
  let races = promiseResults[2];

  //Iterate throught he races
  Object.keys(races).forEach(raceId => {
    if (races[raceId].toScore === true) {
      for (let leg = 1; leg <= races[raceId].numLegs; leg++) { //using human numbers
        let legResults = results.filter(result => {
          return (result.raceId === raceId) && (result.leg === leg);
        });
        if (legResults.length > 0) {
          console.log(`scoring ${raceId}: ${leg}`);
          scoreLeg(legResults, participants);

          races[raceId].legs[leg] = legResults;

          //calculate median leg times
          let sortedLegTimes = legResults.filter(leg => {
            return leg.dnf === false && leg.unknownTime === false;
          })
          .map(leg => {return leg.legTime;})
          .sort((a, b) => {return a - b;});
          let medianLegTime = (sortedLegTimes[Math.floor(sortedLegTimes.length / 2)] +
                               sortedLegTimes[Math.ceil(sortedLegTimes.length / 2)]) / 2;
          races[raceId].legMedianTimes[leg] = medianLegTime;

        }
      }
    }
  });

  console.log('Updating Firebase');
  let fbRef = new Firebase('https://dttdata.firebaseio.com/');
  fbRef.set({
    races: races,
    results: results,
    participants: participants
  });

  //output the ranked list of drivers
  let rankedDrivers = Object.keys(participants)
  .map(key => { return participants[key]; })
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
