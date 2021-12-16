package com.test.spark.reconciler

case class ReconModel(field_name: String = null, matching_record_count: Long = 0, mismatch_record_count: Long = 0,
                      matching_record_percentage: Double = 0.0)
