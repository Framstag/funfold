package com.framstag.funfold.jdbc

import com.framstag.funfold.annotation.PotentialIO
import java.sql.Connection

interface ConnectionProvider {
    @PotentialIO
    fun getConnection():Connection
}