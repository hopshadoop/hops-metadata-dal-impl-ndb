/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/* 
 * File:   Logger.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef LOGGER_H
#define LOGGER_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <istream>
#include <ostream>
#include <string>

using namespace std;

#define LOG_LEVEL_TRACE 0
#define LOG_LEVEL_DEBUG 1
#define LOG_LEVEL_INFO 2
#define LOG_LEVEL_WARN 3
#define LOG_LEVEL_ERROR 4
#define LOG_LEVEL_FATAL 5

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define FORMAT(ITEMS) ((dynamic_cast<ostringstream &>(ostringstream().seekp(0,ios_base::cur) \
               << "(" << __FILENAME__ << ":" << __LINE__ << ":" << __FUNCTION__ << ") " << ITEMS )).str().c_str())
        
#define LOG_TRACE(msg) Logger::trace(FORMAT(msg))
#define LOG_DEBUG(msg) Logger::debug(FORMAT(msg))
#define LOG_INFO(msg)  Logger::info(FORMAT(msg))
#define LOG_WARN(msg)  Logger::warn(FORMAT(msg))
#define LOG_ERROR(msg) Logger::error(FORMAT(msg))
#define LOG_FATAL(msg) Logger::fatal(FORMAT(msg))

#define LOG_NDB_API_ERROR(error) \
        LOG_FATAL("code:" << error.code << ", msg: " << error.message << ".")

class Logger{
public:
    static void setLoggerLevel(int level);
    static void trace(const char* msg);
    static void debug(const char* msg);
    static void info(const char* msg);
    static void warn(const char* msg);
    static void error(const char* msg);
    static void fatal(const char* msg);
    
    static bool isTrace();
private:
    static Logger* mInstance;
    static int mLoggerLevel;
    static void log(const char* level, const char* msg);
};

#endif /* LOGGER_H */

