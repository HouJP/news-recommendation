package com.bda.model

import java.io.File

case class Para(pre_time: String = "1970-01-01",
                time: String = "1970-01-01",
                data: String = "",
                boot: String = "offline",
                host: String = "localhost",
                port: Int = 8124,
                has_docs: Boolean = true,
                has_user: Boolean = true)