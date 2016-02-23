#!/usr/bin/env node
'use strict';

var GoogleSpreadsheet = require('google-spreadsheet');
var Sheet = new GoogleSpreadsheet('1LI4OBEGMPKxBwtGY_xJ1yT_G-rrCJxl8FaC0q0DaBl0');

var Firebase = require('firebase');

var Moment = require('moment-timezone');

// const TS_PATH = './node_modules/jstrueskill/lib/racingjellyfish/jstrueskill';
// var TrueSkillCalculator = require(`${TS_PATH}/TrueSkillCalculator`);
// var GameInfo = require(`${TS_PATH}/GameInfo`).getDefaultGameInfo();
// var Player = require(`${TS_PATH}/Player`);
// var Team = require(`${TS_PATH}/Team`);

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
    let fullname = `${row.last}, ${row.first}`;
    if (row.nickname.length > 0) {fullname += ` (${row.nickname})`;}
    participantMap.set(participantId, {
      fullname: fullname,
      name: {
        lastname: row.last,
        firstname: row.first,
        nickname: row.nickname,
      },
    });
  }
  return participantMap;
}

//Races
function getRaceMap(rows) {
  let raceMap = new Map();
  for (let row of rows) {
    raceMap.set(row.raceid, {
      raceId: row.raceid,
      name: row.racename,
      abbreviation: row.raceabbr,
      date: Moment.tz(row.date, 'MM/DD/YYYY', 'America/New_York').toJSON(),
      numLegs: Number.parseInt(row.numlegs),
      legs: [],
      legMedianTimes: [],
      toScore: row.toscore === 'TRUE' ? true : false
    });
  }
  return raceMap;
}

function getLegResults(rows, raceMap) {
  let results = rows.map((row) => {
    const RACE_DATE = Moment(raceMap.get(row.raceid).date);
    return {
      raceId: row.raceid,
      leg: Number.parseInt(row.leg),
      driverId: Number.parseInt(row.driverid),
      navigatorId: 'n/a',
      start: RACE_DATE.clone().add(Moment.duration(row.starttime)),
      end: RACE_DATE.clone().add(Moment.duration(row.finishtime)),
      duration: Moment.duration(row.dnf === 'TRUE' ? Number.MAX_VALUE : row.legduration),
      rank: null, //placeholder
      dnf: row.dnf === 'TRUE' ? true : false,
      unknownTime: row.unknown === 'TRUE' ? true : false
    };
  });
  return results;
}

// function buildRaceTables(rows, participantMap, raceMap) {
//
//   let raceTables = new Map();
//
//   let results = rows.map((row) => {
//     return {
//       raceId: row.raceid,
//       leg: Number.parseInt(row.leg),
//       driverId: Number.parseInt(row.driverid),
//       navigatorId: 'n/a',
//       startTime: Moment.duration(row.starttime),
//       endTime: Moment.duration(row.finishtime),
//       legTime: Moment.duration(row.legduration),
//       dnf: row.dnf === 'TRUE' ? true : false,
//       unknownTime: row.unknown === 'TRUE' ? true : false
//     };
//   }).sort((r1, r2) => {
//     if (r1.raceId !== r2.raceId) {
//       return r1.date - r2.date;
//     } else if (r1.leg !== r2.leg) {
//       return r1.leg - r2.leg;
//     }
//     let t1 = r1.dnf ? Number.MAX_VALUE : r1.legTime;
//     let t2 = r2.dnf ? Number.MAX_VALUE : r2.legTime;
//     return t1 - t2;
//   }).reduce((results, result) => {
//     if ((results.length === 0) || (results[length - 1][0].raceId !== result.raceId)) {
//       results.push(new Array(raceMap.get(result.raceId).numLegs + 1));
//     }
//     results[results.length - 1][result.leg].push(result);
//   }, []);
//
//   for (let race of raceMap.values()) {
//     //TODO: fix this timezone hack
//     const RACE_DATE = Moment(raceMap.get(race.raceId).date);
//
//     let table = results.filter((result) => {
//       return result.raceId === race.raceId;
//     }).sort((res1, res2) => {
//       if (res1.driverId !== res2.driverId) {
//         return res1.driverId - res2.driverId;
//       } else { return res1.leg - res2.leg; }
//     }).filter((result) => {
//       return result.leg === 1;
//     }).sort((r1, r2) => {
//       //TODO: eliminate duplicated code
//       let t1 = r1.dnf ? Number.MAX_VALUE : r1.legTime;
//       let t2 = r2.dnf ? Number.MAX_VALUE : r2.legTime;
//       return t1 - t2;
//     }).map((result, index, results) => {
//       return {
//         driverId: result.driverId,
//         driverName: participantMap.get(result.driverId).fullname,
//         navigatorName: 'tbd',
//         legStartTime: RACE_DATE.clone().add(result.startTime).format(),
//         legEndTime: RACE_DATE.clone().add(result.endTime).format(),
//         legElapsedTime: result.legTime.toJSON(),
//         legRank: result.dnf ? results.length : index + 1
//       };
//     });
//     raceTables.set(race.raceId, table);
//   }
//
//   return raceTables;
// }

// function scoreLeg(results, participantMap) {
//   results = results.filter(result => result.unknownTime === false)
//   .sort((r1, r2) => {
//     let t1 = r1.dnf ? Number.MAX_VALUE : r1.legTime;
//     let t2 = r2.dnf ? Number.MAX_VALUE : r2.legTime;
//     return t1 - t2;
//   });
//
//   //create teams
//   let teams = results.map(result => {
//     let driver = new Player(result.driverId);
//     let driverSkill = participantMap.get(result.driverId).driverSkill;
//     if (typeof driverSkill === 'undefined') {driverSkill = GameInfo.getDefaultRating();}
//     return new Team(result.driverId.toString(), driver, driverSkill);
//   });
//
//   //create rankings`
//   //TODO: change to a map
//   let finishOrder = [];
//   results.forEach((result, index) => {
//     finishOrder.push(result.dnf ? results.length : index + 1);
//   });
//
//   let newSkills = TrueSkillCalculator.calculateNewRatings(GameInfo, teams, finishOrder);
//
//   //update scores
//   teams.forEach(team => {
//     let player = team.getPlayers()[0];
//     participantMap.get(player.getId()).driverSkill = newSkills[player];
//   });
//
// }

function getRaceResults(legResults, participantMap) {
  let raceResults = legResults
  //first get leg rankings by doing some cool sorting work
  .sort((r1, r2) => {
    if (r1.raceId !== r2.raceId) {
      return r1.date - r2.date;
    } else if (r1.leg !== r2.leg) {
      return r1.leg - r2.leg;
    }
    return r1.duration - r2.duration;
  })
  .map((curResult, index, allResults) => {
    let rank = 1;
    if (index !== 0) {
      let prevResult = allResults[index - 1];
      if ((prevResult.raceId === curResult.raceId) && (prevResult.leg === curResult.leg)) {
        //curResult is in the same leg
        if (curResult.duration === prevResult.duration) {
          rank = prevResult.rank;
        }  else {
          rank = prevResult.rank + 1;
        }
      }
    }
    curResult.rank = rank;
    return curResult;
  })

  //then start to process it into a race result table
  .reduce((races, curLeg) => {
    if (!races.hasOwnProperty(curLeg.raceId)) {
      races[curLeg.raceId] = {};
    }
    if (!races[curLeg.raceId].hasOwnProperty(curLeg.driverId)) {
      races[curLeg.raceId][curLeg.driverId] = {
        driverId: curLeg.driverId,
        driverName: participantMap.get(curLeg.driverId).fullname,
        navigatorName: 'n/a',
        duration: Moment.duration(0),
        rank: null,
        legs: {},
        dnf: false
      };
    }
    races[curLeg.raceId][curLeg.driverId].legs[curLeg.leg] = {
      start: curLeg.start,
      end: curLeg.end,
      duration: curLeg.duration,
      rank: curLeg.rank,
      dnf: curLeg.dnf,
      unknownTime: curLeg.unknownTime
    };
    races[curLeg.raceId][curLeg.driverId].duration.add(curLeg.duration);
    if (curLeg.dnf === true) {
      races[curLeg.raceId][curLeg.driverId].dnf = true;
    }
    return races;
  }, {});

  // convert the participant list to an array sorted by duration
  Object.keys(raceResults).forEach(raceId => {
    raceResults[raceId] = Object.keys(raceResults[raceId])
    .map(driverId => raceResults[raceId][driverId])
    .sort((r1,r2) => {
      let d1 = r1.dnf ? Number.MAX_VALUE : r1.duration;
      let d2 = r2.dnf ? Number.MAX_VALUE : r2.duration;
      return d1 - d2;
    })
    .map((result, index) => {
      // TODO - handle DNFs and unknown times
      result.rank = index + 1;
      return result;
    })
    // HACK - convert all Durations to JSON to fix serialization issue w/Firebase
    .map(result => {
      result.duration = result.duration.toJSON();
      Object.keys(result.legs).forEach(function(leg) {
        result.legs[leg].start = result.legs[leg].start.toJSON();
        result.legs[leg].end = result.legs[leg].end.toJSON();
        result.legs[leg].duration = result.legs[leg].duration.toJSON();
      });
      return result;
    });
  });
  return raceResults;
}

function getDriverStats(raceResults, participantMap) {
  let stats = Object.keys(raceResults).map(key => {return raceResults[key];})
  .reduce((drivers, curRace) => {
    curRace.forEach(driver => {
      if (!drivers.hasOwnProperty(driver.driverId)) {
        drivers[driver.driverId] = {
          id: driver.driverId,
          name: participantMap.get(driver.driverId).fullname,
          starts: 0,
          finishes: 0,
          wins: 0,
          podiums: 0,
          skillRanking: null
        };
      }
      drivers[driver.driverId].starts++;
      if (!driver.dnf) {drivers[driver.driverId].finishes++;}
      if (driver.rank === 1) {drivers[driver.driverId].wins++;}
      if (driver.rank <= 3) {drivers[driver.driverId].podiums++;}
    });
    return drivers;
  }, {});
  return Object.keys(stats).map(key => stats[key]);
}

function main() {
  console.log('Getting data from Google Sheets');
  Promise.all([getSheetRows(Sheet, 1).then(getParticipantMap),
               getSheetRows(Sheet, 3).then(getRaceMap),
               getSheetRows(Sheet, 2)
             ])
  .then((promiseResults) => {
    console.log('Data from sheets pulled');
    let participantMap = promiseResults[0];
    let raceMap = promiseResults[1];
    let legResults = getLegResults(promiseResults[2], raceMap);

    let raceResults = getRaceResults(legResults, participantMap);
    let driverStats = getDriverStats(raceResults, participantMap);

    console.log('Updating Firebase');
    let fbRef = new Firebase('https://dttdata.firebaseio.com/');
    fbRef.set({
      races: Array.from(raceMap.values()),
      raceResults: raceResults,
      driverStats: driverStats
    });

    // //output the ranked list of drivers
    // let rankedDrivers = Array.from(participantMap.values())
    // .filter(a => { return typeof a.driverSkill !== 'undefined';})
    // .sort((a,b) => {
    //   return b.driverSkill.conservativeRating - a.driverSkill.conservativeRating;
    // });
    // rankedDrivers.forEach((driver,index) => {
    //   console.log(`${index} ${driver.driverSkill.conservativeRating.toFixed(3)} ` +
    //               `${driver.driverSkill.mean.toFixed(3)} ` +
    //               `${driver.name.lastname}, ${driver.name.firstname}`);
    // });
    //
    console.log('all done');
  })
  .catch(err => {
    console.error(`ERROR: ${err}`);
    console.error(err.stack);
    throw err;
  });
}

main();
