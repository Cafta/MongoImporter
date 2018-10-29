/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.drcarlosamaral.MongoImporter;

/**
 *
 * @author Nicholas DiPiazza
 */
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

public class DateUtils {

  public static Date asDate(LocalDate localDate) {
    return Date.from(localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
  }

  public static Date asDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  public static LocalDate asLocalDate(Date date) {
    // return Instant.ofEpochMilli(date.getTime()).atOffset(ZoneOffset.UTC).toLocalDate();
    return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
  }

  public static LocalDateTime asLocalDateTime(Date date) {
    //  return Instant.ofEpochMilli(date.getTime()).atOffset(ZoneOffset.UTC).toLocalDateTime();
    return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime();
  }
}