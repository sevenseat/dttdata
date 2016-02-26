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

function getResultCompare(r) {
  if (r.dnf === true) { return Number.MAX_VALUE / 2; }
  if (r.unknownTime === true) { return Number.MAX_VALUE; }
  return Moment.duration(r.duration).asMilliseconds();
}

function rankResults(results) {
  return _(results)
  .groupBy(getResultCompare)
  .toPairs()
  .sort((g1, g2) => {return g1[0] - g2[0];})
  //put these groups back into the result time array, adding their rank
  .reduce((prevResults, curResultGroup) => {
    curResultGroup[1].forEach(result => {
      result.rank = prevResults.length + 1;
    });
    return _.concat(prevResults, curResultGroup[1]);
  }, []);
}

function getRaceResults(legResults, participantMap, raceMap) {
  let raceResults =
  _.chain(legResults)
  .groupBy('raceId')
  .mapValues(race => {
    // console.log(race);
    return _.chain(race)

    ///
    //STEP 1: RANK THE LEGS
    //
    .groupBy('leg') //groups the results by eaech leg
    .mapValues(rankResults) //ranks each leg
    .forOwn(legResult => scoreLeg(legResult, participantMap))
    .reduce((results, legResults) => _.concat(results, legResults), []) //re-flattens legs

    ///
    //STEP 2: BUILD A RESULT TABLE FOR EACH RACE
    //
    .groupBy('driverId')
    .mapValues((legs, driverId) => {
      let driverResult = {
        driverId: driverId,
        driverName: participantMap.get(Number(driverId)).fullname,
        navigatorName: 'n/a',
        rank: null,

        duration: _.reduce(legs, (duration, leg) => {
          return duration.add(Moment.duration(leg.duration));
        }, Moment.duration(0)).toJSON(),

        legs: _.map(legs, leg =>
          _.pick(leg, ['start','end', 'duration', 'rank', 'dnf', 'unknownTime', 'toScore'])),

        // These two parameters are TRUE at the race level if they are true for any leg
        dnf: _.reduce(legs, (dnf, leg) => dnf || leg.dnf, false),
        unknownTime: _.reduce(legs, (unknownTime, leg) => unknownTime || leg.unknownTime, false),
      };

      return driverResult;
    })
    .value();
  })
  //Rank the race results
  .mapValues(rankResults)

  ///
  //STEP 3: ADD CONSOLIDATED INFORMATION ABOUT EACH RACE
  //
  .mapValues((results, raceId) => {
    return _(raceMap.get(raceId))
    .pick(['raceId', 'name', 'theme', 'date', 'start', 'end', 'distance', 'numLegs', 'toScore'])
    .merge({
      results: results,
      time: results[0].duration,
      winner: results[0].driverName
    })
    .value();
  })
  .value();

  // console.log(JSON.stringify(raceResults, null, '\t'));

  return raceResults;
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

function scoreLeg(legResults, participantMap) {
  // console.log(legResults);

  if (legResults.length < 2) {return;}

  let skillData = _(legResults)
  .filter(legResult => (legResult.unknownTime === false))
  .reduce((results, legResult) => {
    let driverId = legResult.driverId;
    results.teams.push(new Team(driverId.toString(),
                       new Player(driverId),
                       participantMap.get(driverId).driverSkill));
    results.ranks.push(legResult.rank);
    return results;
  }, {teams: [], ranks: []});

  let newSkills = TrueSkillCalculator.calculateNewRatings(GameInfo,
                  skillData.teams, skillData.ranks);

  skillData.teams.forEach(team => {
    let player = team.getPlayers()[0];
    participantMap.get(player.getId()).driverSkill = newSkills[player];
  });
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
      let driverId = Number(driver.driverId);
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

    //getScores(raceResults, participantMap);

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
