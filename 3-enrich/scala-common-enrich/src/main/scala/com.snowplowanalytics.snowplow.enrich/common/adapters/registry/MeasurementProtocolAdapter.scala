/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics
package snowplow.enrich.common
package adapters
package registry

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{Resolver, SchemaKey}

// This project
import loaders.CollectorPayload
import utils.ConversionUtils._

/**
 * Transforms a collector payload which conforms to a known version of the Google Analytics
 * protocol into raw events.
 */
object MeasurementProtocolAdapter extends Adapter {

  // for failure messages
  private val vendorName = "MeasurementProtocol"
  private val gaVendor = "com.google.analytics"
  private val vendor = s"$gaVendor.measurement-protocol"
  private val protocolVersion = "v1"
  private val protocol = s"$vendor-$protocolVersion"
  private val format = "jsonschema"
  private val schemaVersion = "1-0-0"

  type Translation = (Function1[String, Validation[String, FieldType]], String)
  case class MPData(schemaUri: String, translationTable: Map[String, Translation])

  sealed trait FieldType
  final case class StringType(s: String) extends FieldType
  final case class IntType(i: Int) extends FieldType
  final case class DoubleType(d: Double) extends FieldType
  final case class BooleanType(b: Boolean) extends FieldType
  implicit val fieldTypeJson4s: FieldType => JValue = (f: FieldType) =>
    f match {
      case StringType(s)  => JString(s)
      case IntType(i)     => JInt(i)
      case DoubleType(f)  => JDouble(f)
      case BooleanType(b) => JBool(b)
    }

  private val idTranslation: (String => Translation) = (fieldName: String) =>
    ((value: String) => StringType(value).success, fieldName)
  private val intTranslation: (String => Translation) = (fieldName: String) =>
    (stringToJInteger(fieldName, _: String).map(i => IntType(i.toInt)), fieldName)
  private val twoDecimalsTranslation: (String => Translation) = (fieldName: String) =>
    (stringToTwoDecimals(fieldName, _: String).map(DoubleType), fieldName)
  private val booleanTranslation: (String => Translation) = (fieldName: String) =>
    (stringToBoolean(fieldName, _: String).map(BooleanType), fieldName)

  private val unstructEventData = Map(
    "pageview" -> MPData(
      SchemaKey(vendor, "page_view", format, schemaVersion).toSchemaUri,
      Map(
        "dl" -> idTranslation("documentLocationURL"),
        "dh" -> idTranslation("documentHostName"),
        "dp" -> idTranslation("documentPath"),
        "dt" -> idTranslation("documentTitle")
      )
    ),
    "screenview" -> MPData(SchemaKey(vendor, "screen_view", format, schemaVersion).toSchemaUri,
      Map("cd" -> idTranslation("name"))),
    "event" -> MPData(
      SchemaKey(vendor, "event", format, schemaVersion).toSchemaUri,
      Map(
        "ec" -> idTranslation("category"),
        "ea" -> idTranslation("action"),
        "el" -> idTranslation("label"),
        "ev" -> intTranslation("value")
      )
    ),
    "transaction" -> MPData(
      SchemaKey(vendor, "transaction", format, schemaVersion).toSchemaUri,
      Map(
        "ti"  -> idTranslation("id"),
        "ta"  -> idTranslation("affiliation"),
        "tr"  -> twoDecimalsTranslation("revenue"),
        "ts"  -> twoDecimalsTranslation("shipping"),
        "tt"  -> twoDecimalsTranslation("tax"),
        "tcc" -> idTranslation("couponCode"),
        "cu"  -> idTranslation("currencyCode")
      )
    ),
    "item" -> MPData(
      SchemaKey(vendor, "item", format, schemaVersion).toSchemaUri,
      Map(
        "ti" -> idTranslation("id"),
        "in" -> idTranslation("name"),
        "ip" -> twoDecimalsTranslation("price"),
        "iq" -> intTranslation("quantity"),
        "ic" -> idTranslation("code"),
        "iv" -> idTranslation("category"),
        "cu" -> idTranslation("currencyCode")
      )
    ),
    "social" -> MPData(
      SchemaKey(vendor, "social", format, schemaVersion).toSchemaUri,
      Map(
        "sn" -> idTranslation("network"),
        "sa" -> idTranslation("action"),
        "st" -> idTranslation("actionTarget")
      )
    ),
    "exception" -> MPData(
      SchemaKey(vendor, "exception", format, schemaVersion).toSchemaUri,
      Map(
        "exd" -> idTranslation("description"),
        "exf" -> booleanTranslation("isFatal")
      )
    ),
    "timing" -> MPData(
      SchemaKey(vendor, "timing", format, schemaVersion).toSchemaUri,
      Map(
        "utc" -> idTranslation("userTimingCategory"),
        "utv" -> idTranslation("userTimingVariableName"),
        "utt" -> intTranslation("userTimingTime"),
        "utl" -> idTranslation("userTimingLabel"),
        "plt" -> intTranslation("pageLoadTime"),
        "dns" -> intTranslation("dnsTime"),
        "pdt" -> intTranslation("pageDownloadTime"),
        "rrt" -> intTranslation("redirectResponseTime"),
        "tcp" -> intTranslation("tcpConnectTime"),
        "srt" -> intTranslation("serverResponseTime"),
        "dit" -> intTranslation("domInteractiveTime"),
        "clt" -> intTranslation("contentLoadTime")
      )
    )
  )

  private val contextData = List(
    MPData(SchemaKey(gaVendor, "undocumented", format, schemaVersion).toSchemaUri,
      List("a", "jid", "gjid").map(e => e -> idTranslation(e)).toMap),
    MPData(SchemaKey(gaVendor, "private", format, schemaVersion).toSchemaUri,
      (List("_v", "_u", "_gid").map(e => e -> idTranslation(e.tail)) ++
        List("_s", "_r").map(e => e -> intTranslation(e.tail))).toMap),
    MPData(SchemaKey(vendor, "general", format, schemaVersion).toSchemaUri,
      Map(
        "v"   -> idTranslation("protocolVersion"),
        "tid" -> idTranslation("trackingId"),
        "aip" -> booleanTranslation("anonymizeIp"),
        "ds"  -> idTranslation("dataSource"),
        "qt"  -> intTranslation("queueTime"),
        "z"   -> idTranslation("cacheBuster")
      )
    ),
    MPData(SchemaKey(vendor, "user", format, schemaVersion).toSchemaUri,
      Map("cid" -> idTranslation("clientId"), "uid" -> idTranslation("userId"))),
    MPData(SchemaKey(vendor, "session", format, schemaVersion).toSchemaUri,
      Map(
        "sc"    -> idTranslation("sessionControl"),
        "uip"   -> idTranslation("ipOverride"),
        "ua"    -> idTranslation("userAgentOverride"),
        "geoid" -> idTranslation("geographicalOverride")
      )
    ),
    MPData(SchemaKey(vendor, "traffic_source", format, schemaVersion).toSchemaUri,
      Map(
        "dr"    -> idTranslation("documentReferrer"),
        "cn"    -> idTranslation("campaignName"),
        "cs"    -> idTranslation("campaignSource"),
        "cm"    -> idTranslation("campaignMedium"),
        "ck"    -> idTranslation("campaignKeyword"),
        "cc"    -> idTranslation("campaignContent"),
        "ci"    -> idTranslation("campaignId"),
        "gclid" -> idTranslation("googleAdwordsId"),
        "dclid" -> idTranslation("googleDisplayAdsId")
      )
    ),
    MPData(SchemaKey(vendor, "system_info", format, schemaVersion).toSchemaUri,
      Map(
        "sr" -> idTranslation("screenResolution"),
        "vp" -> idTranslation("viewportSize"),
        "de" -> idTranslation("documentEncoding"),
        "sd" -> idTranslation("screenColors"),
        "ul" -> idTranslation("userLanguage"),
        "je" -> booleanTranslation("javaEnabled"),
        "fl" -> idTranslation("flashVersion")
      )
    ),
    MPData(SchemaKey(vendor, "link", format, schemaVersion).toSchemaUri,
      Map("linkid" -> idTranslation("id"))),
    MPData(SchemaKey(vendor, "app", format, schemaVersion).toSchemaUri,
      Map(
        "an"   -> idTranslation("name"),
        "aid"  -> idTranslation("id"),
        "av"   -> idTranslation("version"),
        "aiid" -> idTranslation("installerId")
      )
    ),
    MPData(SchemaKey(vendor, "product_action", format, schemaVersion).toSchemaUri,
      Map(
        "pa"  -> idTranslation("productAction"),
        "pal" -> idTranslation("productActionList"),
        "cos" -> intTranslation("checkoutStep"),
        "col" -> idTranslation("checkoutStepOption")
      )
    ),
    MPData(SchemaKey(vendor, "content_experiment", format, schemaVersion).toSchemaUri,
      Map("xid" -> idTranslation("id"), "xvar" -> idTranslation("variant")))
  )

  // layer of indirection linking fields to schemas
  private val fieldToSchemaMap = contextData
    .flatMap(mpData => mpData.translationTable.keys.map(_ -> mpData.schemaUri))
    .toMap

  private val directMappings = (hitType: String) => Map(
    "uip" -> "ip",
    "dr"  -> "refr",
    "de"  -> "cs",
    "sd"  -> "cd",
    "ul"  -> "lang",
    "je"  -> "f_java",
    "dl"  -> "url",
    "dt"  -> "page",
    "ti"  -> (if (hitType == "transaction") "tr_id" else "ti_id"),
    "ta"  -> "tr_af",
    "tr"  -> "tr_tt",
    "ts"  -> "tr_sh",
    "tt"  -> "tr_tx",
    "in"  -> "ti_nm",
    "ip"  -> "ti_pr",
    "iq"  -> "ti_qu",
    "ic"  -> "ti_sk",
    "iv"  -> "ti_ca",
    "cu"  -> (if (hitType == "transaction") "tr_cu" else "ti_cu")
  )

  private val letThroughFields = List("ua")

  /**
   * Converts a CollectorPayload instance into raw events.
   * @param payload The CollectorPaylod containing one or more raw events as collected by
   * a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      s"Querystring is empty: no $vendorName event to process".failNel
    } else {
      params.get("t") match {
        case None => s"No $vendorName t parameter provided: cannot determine hit type".failNel
        case Some(hitType) =>
          for {
            // unstruct event
            trTable             <- unstructEventData.get(hitType).map(_.translationTable)
              .toSuccess(s"No matching $vendorName hit type for hit type $hitType".wrapNel)
            schema              <-
              lookupSchema(hitType.some, vendorName, unstructEventData.mapValues(_.schemaUri))
            unstructEvent       <- translatePayload(params, trTable)
            unstructEventJson    = buildJson(schema, unstructEvent)
            unstructEventParams  =
              Map("e" -> "ue", "ue_pr" -> compact(toUnstructEvent(unstructEventJson)))
            // contexts
            contexts     <- buildContexts(params, contextData, fieldToSchemaMap)
            contextJsons = contexts.map(c => buildJson(c._1, c._2))
            contextParam =
              if (contextJsons.isEmpty) Map.empty
              else Map("co" -> compact(toContexts(contextJsons.toList)))
            // direct mappings
            mappings = translatePayload(params, directMappings(hitType))
            // TODO: let-through fields
          } yield RawEvent(
            api         = payload.api,
            parameters  = unstructEventParams ++ contextParam ++ mappings,
            contentType = payload.contentType,
            source      = payload.source,
            context     = payload.context
          ).wrapNel
      }
    }
  }

  /**
   * Translates a payload according to a translation table.
   * @param originalParams original payload in key-value format
   * @param translationTable mapping between original params and the wanted format
   * @return a translated params
   */
  private def translatePayload(
    originalParams: Map[String, String],
    translationTable: Map[String, Translation]
  ): ValidationNel[String, Map[String, FieldType]] =
    originalParams.foldLeft(Map.empty[String, ValidationNel[String, FieldType]]) {
      case (m, (fieldName, value)) =>
        translationTable
          .get(fieldName)
          .map { case (translation, newName) =>
            m + (newName -> translation(value).toValidationNel)
          }
          .getOrElse(m)
    }.sequenceU

  /**
   * Translates a payload according to a translation table.
   * @param originalParams original payload in key-value format
   * @param translationTable mapping between original params and the wanted format
   * @return a translated params
   */
  private def translatePayload(
    originalParams: Map[String, String],
    translationTable: Map[String, String]
  ): Map[String, String] =
    originalParams.foldLeft(Map.empty[String, String]) {
      case (m, (fieldName, value)) =>
        translationTable
          .get(fieldName)
          .map(newName => m + (newName -> value))
          .getOrElse(m)
    }

  /**
   * Discovers the contexts in the payload in linear time (size of originalParams).
   * @param originalParams original payload in key-value format
   * @param referenceTable list of context schemas and their associated translation
   * @param fieldToSchemaMap reverse indirection from referenceTable linking fields with the MP
   * nomenclature to schemas
   * @return a map containing the discovered contexts keyed by schema
   */
  private def buildContexts(
    originalParams: Map[String, String],
    referenceTable: List[MPData],
    fieldToSchemaMap: Map[String, String]
  ): ValidationNel[String, Map[String, Map[String, FieldType]]] = {
    val refTable = referenceTable.map(d => d.schemaUri -> d.translationTable).toMap
    originalParams.foldLeft(Map.empty[String, Map[String, ValidationNel[String, FieldType]]]) {
      case (m, (fieldName, value)) =>
        fieldToSchemaMap.get(fieldName).map { schema =>
          // this is safe when fieldToSchemaMap is built from referenceTable
          val (translation, newName) = refTable(schema)(fieldName)
          val trTable = m.getOrElse(schema, Map.empty) +
            (newName -> translation(value).toValidationNel)
          m + (schema -> trTable)
        }
        .getOrElse(m)
    }
    .map { case (k, v) => (k -> v.sequenceU) }
    .sequenceU
  }

  private def buildJson(schema: String, fields: Map[String, FieldType]): JValue =
    ("schema" -> schema) ~ ("data" -> fields)
}