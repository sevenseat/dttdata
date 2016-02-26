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

var _ = require('lodash');

function getSheetRows(sheet, tab) {
  return new Promise((resolve,reject) => {
    Sheet.getRows(tab, (err, rows) => {
      if (err !== null) {reject(err);}
      resolve(rows);
    });
  });
}

//Participants
function getParticipantMap(rows) {
  return rows.reduce((participantMap, row) => {
    participantMap.set(Number.parseInt(row.participantid), {
      fullname: `${row.last}, ${row.first}`,
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
      date: Moment.tz(row.date, 'MM/DD/YYYY', 'America/New_York').toJSON(),
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
      start: RACE_DATE.clone().add(Moment.duration(row.starttime)).toJSON(),
      end: RACE_DATE.clone().add(Moment.duration(row.finishtime)).toJSON(),
      duration: Moment.duration(row.legduration).toJSON(),
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
    return Moment.duration(r.duration).asMilliseconds();
  });
  return compareVal(r1) - compareVal(r2);
}

function getRaceResults(legResults, participantMap, raceMap) {
  let raceResults = _.chain(legResults)
  //first get leg rankings by doing some cool sorting work
  .sort((r1, r2) => {
    if (r1.raceId !== r2.raceId) { return new Date(r1.start) - new Date(r2.start); }
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
        duration: Moment.duration(0).toJSON(),
        rank: null,
        legs: {},
        dnf: false,
        unknownTime: false
      };
    }
    races[curLeg.raceId][curLeg.driverId].legs[curLeg.leg] = _.pick(curLeg,
      ['start','end', 'duration', 'rank', 'dnf', 'unknownTime', 'toScore']);

    Moment.duration(races[curLeg.raceId][curLeg.driverId].duration).add(curLeg.duration);
    if (curLeg.dnf === true) { races[curLeg.raceId][curLeg.driverId].dnf = true; }
    if (curLeg.unknownTime === true) { races[curLeg.raceId][curLeg.driverId].unknownTime = true; }
    return races;
  }, {})

  //create sorted table array of results
  .reduce((resultsMap, raceResult, raceId) => {
    resultsMap[raceId] = _(raceResult).values()
    .sort(resultCompare)
    .map((result, index, prevResults) => {
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
    .value();
    return resultsMap;
  }, {})
  .value();

  let retVal = _(Array.from(raceMap.values()))
  .map(race => {
    var raceDetail = _.pick(race,
        ['raceId', 'name', 'theme', 'date', 'start', 'end', 'distance', 'numLegs', 'toScore']);
    let raceId = raceDetail.raceId;
    if (raceResults[raceId] !== undefined) {
      let results = raceResults[raceId];
      raceDetail.results = results;
      raceDetail.time = results[0].duration;
      raceDetail.winner = results[0].driverName;
    }
    return raceDetail;
  })
  .value();

  // console.log(retVal);

  return retVal;
}

function getRaceList(raceResults) {
  //TODO: figure out how to chain this... couldn't figure out how to make it work
  let list =  _.cloneDeep(raceResults);

  // I wanted to use lo_dash "omit", but it didnt' work
  return _.map(list, race => {
    delete race.results;
    return race;
  })
  .sort((r1, r2) => { return new Date(r1.date) - new Date(r2.date);});
}

function getScores(raceResults, participantMap) {
  _(raceResults)
  .filter(result => {return (result.toScore === true);})
  .forEach(race =>
    _.range(1, race.numLegs).forEach(leg => {
      let legResults = race.results
      .filter(raceResult => {
        return (raceResult.legs[leg] !== undefined) &&
              //doing it on the race level to reduce the frequency of scoring
              //iteration problems
               (raceResult.unknownTime === false);
      })
      .reduce((legResults, raceResult) => {
        let driverId = raceResult.driverId;
        legResults.teams.push(new Team(driverId.toString(),
                                       new Player(driverId),
                                       participantMap.get(driverId).driverSkill));
        legResults.ranks.push(raceResult.legs[leg].rank);
        return legResults;
      }, {teams: [], ranks: []});

      let newSkills = TrueSkillCalculator.calculateNewRatings(GameInfo,
                      legResults.teams, legResults.ranks);

      legResults.teams.forEach(team => {
        let player = team.getPlayers()[0];
        participantMap.get(player.getId()).driverSkill = newSkills[player];
      });
    })
  );
}

function getDriverStats(raceResults, participantMap) {
  // console.log(JSON.stringify(raceResults, null, '\t'));
  let stats = _(raceResults)
  .values()
  .filter(race => {return _.has(race, 'results'); })
  .map(race => {return race.results;})
  .reduce((drivers, curRace) => {
    // console.log(curRace);
    curRace.forEach(driver => {
      let driverId = driver.driverId;
      if (!drivers.has(driverId)) {
        drivers.set(driverId, {
          id: driverId,
          name: participantMap.get(driverId).fullname,
          starts: 0,
          finishes: 0,
          wins: 0,
          podiums: 0,
          driverSkill: participantMap.get(driverId).driverSkill.conservativeRating.toFixed(2),
          rank: null,
        });
      }

      let thisDriver = drivers.get(driverId);
      thisDriver.starts++;
      if (!driver.dnf) {thisDriver.finishes++;}
      if (driver.rank === 1) {thisDriver.wins++;}
      if (driver.rank <= 3) {thisDriver.podiums++;}
    });
    return drivers;
  }, new Map());
  return Array.from(stats.values())
  .sort((d1, d2) => {return d2.driverSkill - d1.driverSkill;})
  .map((driver, index) => {
    driver.rank = index + 1;
    return driver;
  });
}

function printDriverStats(driverStats) {
  // //output the ranked list of drivers
  let rankedDrivers = driverStats
  .filter(a => { return typeof a.driverSkill !== 'undefined';})
  .sort((a,b) => {return b.driverSkill - a.driverSkill;});

  rankedDrivers.forEach((driver,index) => {
    let rating = driver.driverSkill;
    console.log(`${index}\t${rating}\t${driver.starts}\t${driver.name}`);
  });
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

    getScores(raceResults, participantMap);

    let driverStats = getDriverStats(raceResults, participantMap);

    console.log('Updating Firebase');
    let fbRef = new Firebase('https://dttdata.firebaseio.com/');
    fbRef.set({
      races: raceList,
      raceResults: raceResults,
      driverStats: driverStats
    });

    printDriverStats(driverStats);

    console.log('all done');
  })
  .catch(err => {
    console.error(`ERROR: ${err}`);
    console.error(err.stack);
    throw err;
  });
}

main();
