function OnUpdate(doc, meta) {
    log("original document: ", doc);

    try {
      // transform document
      var newDoc = transformValues(null, "", doc);

      // add a type attribute based on the document ID (available in meta.id)
      newDoc["type"] = getTypeFromId(meta.id);

      // generate a document ID for the transformed document based on the type and the _id attribute value
      var id = generateId(newDoc);
      log("transformed document (id = " + id + "): ", newDoc);

      // write transformed document to target bucket with generated ID
      target[id] = newDoc;
    } catch (e) {
      log("error transforming document " + meta.id + ". See error bucket for more details.");

      // if there are any errors, store error message in the error attribute
      doc["error"] = e;

      // write untransformed document to error bucket with original ID
      error[meta.id] = doc;
    }
}

function OnDelete(meta) {
}

// This is a recursive function that will iterate over all properties in the document (including arrays & sub-objects)
// It will transform Extended JSON to standard JSON.
function transformValues(parentObj, parentProperty, obj) {
  var propertyType = "";

  // for every property in the object
  for (var property in obj) {
    if (obj.hasOwnProperty(property) && obj[property] != null) {
      switch (property) {
        case "$oid":
          // convert parentObj.parentProperty = {"$oid":"3487634876"}
          // to parentObj.parentProperty = "3487634876"
          parentObj[parentProperty] = obj[property];
          break;

        case "$date":
          // convert parentObj.parentProperty = {"$date":{"$numberLong":"-2418768000000"}}
          // to parentObj.parentProperty = -2418768000000
          parentObj[parentProperty] = Number(obj["$date"]["$numberLong"]);
          break;

        case "$numberInt":
        case "$numberDecimal":
        case "$numberLong":
        case "$numberDouble":
          // convert parentObj.parentProperty = {"$numberInt":"1"}
          // to parentObj.parentProperty = 1
          parentObj[parentProperty] = Number(obj[property]);
          break;

        // !!! This function can be extended by adding additional case statements here !!!

        default:
          // otherwise, check the property type
          propertyType = determineType(obj[property]);
          switch (propertyType) {
            case "Object":
              // if the property is an object, recursively transform the object
              transformValues(obj, property, obj[property]);
              break;

            case "Array":
              // if the property is an array, transform every element in the array
              transformArray(obj[property]);
              break;

            default:
              // otherwise, do nothing
              break;
          }
      }
    }
  }

  return obj;
}

// Determine the type of the specified object
function determineType(obj) {
  return obj == null ? "null" : obj.constructor.name;
}

// Transform every element in the specified array
function transformArray(obj) {
  for (var i = 0; i < obj.length; i++) {
    transformValues(obj, i, obj[i]);
  }
}

// Get document type from specified id.
// This function expects that documents will be imported with IDs in the following format:
// example: <document type>:12
function getTypeFromId(id) {
  return id.split(":")[0];
}

// Generate a document ID for the specified document.
// The new ID will be based on the value of the type attribute and the value of the _id attribute:
// <type>:<_id>
function generateId(document) {
  var documentId = document["_id"];
  if (determineType(documentId) != "String") {
    throw "'_id' value must be a String: _id = '" + documentId + "'";
  }

  return document["type"] + ":" + documentId;
}
