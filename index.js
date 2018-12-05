const   axios =     require('axios'),
        mongoose =  require('mongoose'),
        fs =        require('fs');
// mongoose.connect('mongodb://localhost:27017/mineraldat');

async function fetchMineralNames() {
    let charsToFetch = 1;
    let mineralNames = [];
    for (let i = 0; i < charsToFetch; i++) {
        const response = await axios.get(`https://en.wikipedia.org/w/api.php?action=parse&page=List%20of%20minerals&section=${i + 1}&prop=links&format=json`);
        let currentGroupIndex = 0;
        let currentNameIndex = 0;
        for (let j = 0; j < response.data.parse.links.length; j++) {
            if (response.data.parse.links[j]['*'].toLowerCase().charAt(0) === String.fromCharCode(97 + i)) {

                if (currentNameIndex % 50 === 0) {
                    if (currentNameIndex !== 0) {
                        currentGroupIndex++;
                    } 
                    mineralNames[currentGroupIndex] = response.data.parse.links[j]['*'];
                } else {
                    mineralNames[currentGroupIndex] += `|${response.data.parse.links[j]['*']}`;
                }
                currentNameIndex++;
            }
        }    
    }
    console.log(`Mineral Names Fetching completed`);
    return mineralNames;
}

async function fetchMineralData(mineralNames, redirectIteration = 0) {
    console.log(mineralNames[0]);
    var redirects = [];
    var hashRedirects = [];
    let failedMinerals = [];
    let mineralData = {};
    let redirectsCount = 0;
    let currentRedirectIndex = 0;

    for (let i = 0; i < mineralNames.length; i++) {
        const response = await axios.get(`https://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&titles=${mineralNames[i]}&rvsection=0`);
        if (response.data.error && response.data.error.code) {
            if (response.data.error.code === 'nosuchsection') {
                console.log(`failed to fetch all mineral data - no such section`);
                return null;
            }
        } else {
            let pageKeys = Object.keys(response.data.query.pages);
            for (let i = 0; i < pageKeys.length; i++) {
                let queryData = response.data.query.pages[pageKeys[i]];
                if (queryData.revisions[0]['*'].toLowerCase().includes('#redirect')) {
                    if (queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1].includes('#')) {
                        hashRedirects.push(queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1]);
                    } else {
                        if (redirectsCount % 50 === 0) {
                            if (redirectsCount !== 0) {
                                currentRedirectIndex++;
                            }
                            redirects[currentRedirectIndex] = queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1];
                        } else {
                            redirects[currentRedirectIndex] += `|${queryData.revisions[0]['*'].match(/\[\[(.*?)\]/)[1]}`;
                        }
                        redirectsCount++;
                    }
                } else {
                    let tempMineralData = {};
                    let mineralName = queryData.title;
                    let rawData = queryData.revisions[0]['*'];
                    console.log(`${mineralName} parsing begin`);
                    if (rawData.includes(`\n'''`)) {
                        rawData = rawData.substring(0, rawData.indexOf(`\n'''`));
                    }
                    rawData = rawData.replace(/(<ref[^>|\/]*>.*?<\/ref>)|<ref[^>]*\/>|'''/gm, '');

                    let splitData = rawData.split(/\s\|\s/);
                    
                    splitData.forEach(function(fetchedData) {
                        fetchedData = fetchedData.trim();
                        if (fetchedData.search(/\s\=\s/) !== -1) {
                            let splitKeyVals = fetchedData.split(/\s\=\s/);
                            tempMineralData[splitKeyVals[0].trim()] = splitKeyVals[1].replace(/\r?\n|\r/, '').trim();
                        }
                    });

                    if (!tempMineralData['name'] || 
                        (!tempMineralData['name'].toLowerCase().includes(mineralName.toLowerCase()) && !mineralName.toLowerCase().includes(tempMineralData['name'].toLowerCase()))) {
                        failedMinerals.push(queryData.title);
                        console.log(`${mineralName} parsing failed - No name attribute`);
                    } else {
                        mineralData[mineralName] = tempMineralData;
                        console.log(`${mineralName} parsed successully`);
                    }
                }
            }
        }
    }
    if (hashRedirects.length > 0 && redirects.length > 0) {
        redirects = [...redirects, ...hashRedirects];
    } else if (hashRedirects.length > 0) {
        redirects = hashRedirects;
    }
    if (redirects.length > 0 && redirectIteration < 5) {
        console.log(`Processing redirects - Group ${redirectIteration}`);
        let redirectData = await fetchMineralData(redirects, ++redirectIteration);
        mineralData = {...mineralData, ...redirectData.mineralData};
        failedMinerals = [...failedMinerals, ...redirectData.failedMinerals];
    }

    return {
        mineralData: mineralData, 
        failedMinerals: failedMinerals
    };
}

async function writeMineralProperties(mineralData) {
    let mineralAttributes = ["name", "class", "category", "color", "habit", "twinning", "cleavage", "fracture", "tenacity", "mohs", "luster", "streak", "diaphaneity", "gravity", "refractive", "birefringence", "pleochroism", "dispersion", "solubility", "fluorescence", "twoV", "formula", "molweight", "strunz", "dana", "crystalSystem", "unitCell", "symmetry", "opticalprop", "image"];
    let stream = fs.createWriteStream('minerals.csv');
    await stream.once('open', function(fd) {
        for (let i = 0; i < mineralAttributes.length; i++) {
            stream.write(mineralAttributes[i]);
            if (i < mineralAttributes.length - 1) {
                stream.write('~');
            }
        }
        stream.write('\n');
        let minNames = Object.keys(mineralData);

        for (let i = 0; i < minNames.length; i++) {
            if (!mineralData[minNames[i]]["color"] && mineralData[minNames[i]]["colour"]) {
                mineralData[minNames[i]]["color"] = mineralData[minNames[i]]["colour"];
            }
            if (!mineralData[minNames[i]]["luster"] && mineralData[minNames[i]]["lustre"]) {
                mineralData[minNames[i]]["luster"] = mineralData[minNames[i]]["lustre"];
            }
            if (mineralData[minNames[i]]["2v"]) {
                mineralData[minNames[i]]["twoV"] = mineralData[minNames[i]]["2v"];
            }
            if (mineralData[minNames[i]]["system"]) {
                mineralData[minNames[i]]["crystalSystem"] = mineralData[minNames[i]]["system"];
            }
            if (mineralData[minNames[i]]["unit cell"]) {
                mineralData[minNames[i]]["unitCell"] = mineralData[minNames[i]]["unit cell"];
            }
            if (!mineralData[minNames[i]]["molweight"]) {
                if (mineralData[minNames[i]]["molecular weight"]) {
                    mineralData[minNames[i]]["molweight"] = mineralData[minNames[i]]["molecular weight"];
                } else if (mineralData[minNames[i]]["molarmass"]) {
                    mineralData[minNames[i]]["molweight"] = mineralData[minNames[i]]["molarmass"];
                }
            }
            if (mineralData[minNames[i]]["class"]) {
                let classValue = mineralData[minNames[i]]["class"].replace(/<small>|<\/small>/, '');
                if (classValue.includes("[[H-M symbol]]")) {
                    
                    // [[H-M symbol]]: (overline|#)
                    // [[H-M symbol]]: (?) (?)* (?)* (overline|#)*
                    // (same [[H-M symol]])
                    // overline|# = #\u0305
                    mineralData[minNames[i]]["class"] = mineralData[minNames[i]]["class"].replace(/\[\[|\}\}|<small>|<\/small>/gm, '');
                }
            }
            if (mineralData[minNames[i]]["mohs"]) {
                mineralData[minNames[i]]["mohs"] = mineralData[minNames[i]]["mohs"].replace(/frac\|\b(\d)\b\|1\|2/gm, function(match) {
                    return `${match.substring(5, 6)}.5`;
                });
                mineralData[minNames[i]]["mohs"] = mineralData[minNames[i]]["mohs"].replace(/(&nbsp;|\s)*(-|&ndash;|â€“|\\u2013|to)(&nbsp;|\s)*/gm, '-');
            }

            for (let j = 0; j < mineralAttributes.length; j++) {
                if (mineralData[minNames[i]][mineralAttributes[j]]) {
                    stream.write(mineralData[minNames[i]][mineralAttributes[j]].replace(/~|<br\s*?\/>|\{\{|\}\}/gm, ''));
                } 
                if (j < mineralAttributes.length - 1) {
                    stream.write('~');
                } else if (i < minNames.length - 1) {
                    stream.write('\n');
                }
            }
        }
    });
    // console.log(`Completed writing mineral properties to csv`);
}

function catalogUniqueAttributes(mineralData) {
    let mineralAttributes = [];
    let mineralNames = Object.keys(mineralData);

    for (let i = 0; i < mineralNames.length; i++) {
        Object.keys(mineralData[mineralNames[i]]).forEach(function(attribute) {
            if (!mineralAttributes.includes(attribute)) {
                mineralAttributes.push(attribute);
            }
        });
    }
    // console.log(`Completed cataloging unique mineral attributes`);
    return mineralAttributes;
}

async function writeAttributes(attributes) {
    let stream = fs.createWriteStream('mineralsAttributes.txt');
    await stream.once('open', function(fd) {
        for (let i = 0; i < attributes.length; i++) {
            stream.write(attributes[i]);
            if (i < attributes.length - 1) {
                stream.write('\n');
            }
        }
    });
    // console.log(`Completed writing mineral attributes to txt`);
}

async function writeFailedMinerals(failedMinerals) {
    let stream = fs.createWriteStream('failedMinerals.txt');
    await stream.once('open', function(fd) {
        for (let i = 0; i < failedMinerals.length; i++) {
            stream.write(failedMinerals[i]);
            if (i < failedMinerals.length - 1) {
                stream.write('\n');
            }
        }
    });
    // console.log(`Completed writing failed minerals to txt`);
}

async function processMinerals() {
    let mineralNames = [];
    let fetchedData = {};
    let mineralAttributeList = [];

    mineralNames = await fetchMineralNames();
    fetchedData = await fetchMineralData(mineralNames);
    
    mineralAttributeList = catalogUniqueAttributes(fetchedData.mineralData);

    console.log(`Writing mineral properties to csv`);
    await writeMineralProperties(fetchedData.mineralData);

    console.log(`Writing failed minerals to txt`);
    await writeFailedMinerals(fetchedData.failedMinerals);

    console.log(`Cataloging unique attributes`);
    mineralAttributeList = catalogUniqueAttributes(fetchedData.mineralData);

    console.log(`Writing unique mineral attributes to txt`);
    await writeAttributes(mineralAttributeList);
}

processMinerals();
