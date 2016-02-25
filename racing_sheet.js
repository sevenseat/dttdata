#!/usr/bin/env node
'use strict';

// spreadsheet key is the long id in the sheets URL
var GoogleSpreadsheet = require('google-spreadsheet');
var Sheet = new GoogleSpreadsheet('1LI4OBEGMPKxBwtGY_xJ1yT_G-rrCJxl8FaC0q0DaBl0');

var Firebase = require('firebase');

var Moment = require('moment-timezone');

const TS_PATH = './node_modules/jstrueskill/lib/racingjellyfish/jstrueskill';
var TrueSkillCalculator = require(`${TS_PATH}/TrueSkillCalculator`);
var GameInfo = require(`${TS_PATH}/GameInfo`).getDefaultGameInfo();
var Player = require(`${TS_PATH}/Player`);
var Team = require(`${TS_PATH}/Team`);

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
  return rows.reduce((participantMap, row) => {
    let participantId = Number.parseInt(row.participantid);
    let fullname = `${row.last}, ${row.first}`;
    // if (row.nickname.length > 0) {fullname += ` (${row.nickname})`;}
    participantMap.set(participantId, {
      fullname: fullname,
      name: {
        lastname: row.last,
        firstname: row.first,
        nickname: row.nickname,
      },
      driverSkill: GameInfo.getDefaultRating()
    });
    return participantMap;
  }, new Map());
}

//Races
function getRaceMap(rows) {
  return rows.reduce((raceMap, row) => {
    raceMap.set(row.raceid, {
      raceId: row.raceid,
      name: `${row.year} ${row.racename}`,
      theme: row.theme,
      abbreviation: row.raceabbr,
      date: Moment.tz(row.date, 'MM/DD/YYYY', 'America/New_York').toDate(),
      numLegs: Number.parseInt(row.numlegs),
      legs: [],
      toScore: row.toscore === 'TRUE' ? true : false,
      start: row.start,
      finish: row.finish,
      distance: row.distance
    });
    return raceMap;
  }, new Map());
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
      duration: Moment.duration(row.legduration),
      rank: null, //placeholder
      dnf: row.dnf === 'TRUE' ? true : false,
      unknownTime: row.unknown === 'TRUE' ? true : false
    };
  });
  return results;
}

function resultCompare(r1, r2) {
  let compareVal = (r => {
    if (r.dnf === true) { return Number.MAX_VALUE / 2; }
    if (r.unknownTime === true) { return Number.MAX_VALUE; }
    return r.duration.asMilliseconds();
  });
  return compareVal(r1) - compareVal(r2);
}

function getRaceResults(legResults, participantMap, raceMap) {
  let raceResults = legResults
  //first get leg rankings by doing some cool sorting work
  .sort((r1, r2) => {
    if (r1.raceId !== r2.raceId) { return r1.start - r2.start; }
    if (r1.leg !== r2.leg) { return r1.leg - r2.leg; }
    return resultCompare(r1, r2);
  })
  //rank the individual race legs
  .map((curResult, index, allResults) => {
    let rank = 1;
    if (index !== 0) {
      let prevResult = allResults[index - 1];
      if ((prevResult.raceId === curResult.raceId) && (prevResult.leg === curResult.leg)) {
        //curResult is in the same leg
        if (resultCompare(curResult, prevResult) === 0) {
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
        dnf: false,
        unknownTime: false
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
    if (curLeg.dnf === true) { races[curLeg.raceId][curLeg.driverId].dnf = true; }
    if (curLeg.unknownTime === true) { races[curLeg.raceId][curLeg.driverId].unknownTime = true; }
    return races;
  }, {});

  // convert the participant list to an array sorted by total duration
  Object.keys(raceResults).forEach(raceId => {
    raceResults[raceId] = Object.keys(raceResults[raceId])
    .map(driverId => raceResults[raceId][driverId])
    .sort(resultCompare)
    .map((result, index, prevResults) => {
      // TODO - handle DNFs and unknown times
      if (index === 0) {
        result.rank = 1;
      } else {
        let prevResult = prevResults[index - 1];
        if (resultCompare(result, prevResult) === 0) {
          result.rank = prevResult.rank;
        } else {
          result.rank = prevResult.rank + 1;
        }
      }
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

  return Object.keys(raceResults)
  .reduce((results, raceId) => {
    let result = raceResults[raceId];
    results[raceId] = {
      raceId: raceId,
      name: raceMap.get(raceId).name,
      theme: raceMap.get(raceId).theme,
      date: raceMap.get(raceId).date.toJSON(),
      start: raceMap.get(raceId).start,
      end: raceMap.get(raceId).finish,
      distance: raceMap.get(raceId).distance,
      numLegs: raceMap.get(raceId).numLegs,
      time: result[0].duration,
      // speed: raceMap.get(raceId).distance /
      // Moment.duration(result[0].duration).asSeconds() / 3600,
      winner: result[0].driverName,
      results: raceResults[raceId]
    };
    return results;
  }, {});

}

function getRaceList(raceResults) {

  //deep copy
  let clonedResults = JSON.parse(JSON.stringify(raceResults)) ;

  let list = Object.keys(clonedResults)
  .map(raceId => {
    delete clonedResults[raceId].results;
    return clonedResults[raceId];
  })
  .sort((r1, r2) => {
    return new Date(r1.date) - new Date(r2.date);
  });

  return list;
}

function getScores(raceResults, participantMap) {

  Object.keys(raceResults).map(key => {return raceResults[key];})
  .forEach(race => {
    for (let leg = 1; leg <= race.numLegs; leg++) {
      let results = race.results
      .filter(result => {return result.unknownTime === false;})
      .filter(result => {return result.legs[leg] !== undefined;})
      .map(result => {
        console.log(`${race.name} ${leg} ${result.driverId}`);
        return {
          driverId: result.driverId,
          rank: result.legs[leg].rank,
        };
      })
      .sort((r1, r2) => {return r2.rank - r1.rank;})
      .map(result => {
        let driver = new Player(result.driverId);
        let driverSkill = participantMap.get(result.driverId).driverSkill;
        let team =  new Team(result.driverId.toString(), driver, driverSkill)
        return [team, result.rank]
      })
      .reduce((result, item) => {
        result.teams.push(item[0]);
        result.ranks.push(item[1]);
        return result;
      }, {teams: [], ranks: []});

      console.log(`${race.name} ${leg} ${results.teams.length}`);
      let newSkills = TrueSkillCalculator.calculateNewRatings(GameInfo,
            results.teams, results.ranks);

      results.teams.forEach(team => {
        let player = team.getPlayers()[0];
        participantMap.get(player.getId()).driverSkill = newSkills[player];
      });

    }
  });


}

function getDriverStats(raceResults, participantMap) {
  let stats = Object.keys(raceResults).map(key => {return raceResults[key].results;})
  .reduce((drivers, curRace) => {
    curRace.forEach(driver => {
      if (!drivers.has(driver.driverId)) {
        drivers.set(driver.driverId, {
          id: driver.driverId,
          name: participantMap.get(driver.driverId).fullname,
          starts: 0,
          finishes: 0,
          wins: 0,
          podiums: 0,
          skillRanking: null
        });
      }

      let thisDriver = drivers.get(driver.driverId);
      thisDriver.starts++;
      if (!driver.dnf) {thisDriver.finishes++;}
      if (driver.rank === 1) {thisDriver.wins++;}
      if (driver.rank <= 3) {thisDriver.podiums++;}
    });
    return drivers;
  }, new Map());
  return Array.from(stats.values());
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

    let raceResults = getRaceResults(legResults, participantMap, raceMap);
    let raceList = getRaceList(raceResults, raceMap);

    let driverScores = getScores(raceResults, participantMap);

    let driverStats = getDriverStats(raceResults, participantMap);

    console.log('Updating Firebase');
    let fbRef = new Firebase('https://dttdata.firebaseio.com/');
    fbRef.set({
      races: raceList,
      raceResults: raceResults,
      driverStats: driverStats
    });

    // //output the ranked list of drivers
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
}

main();
