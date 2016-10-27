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

#include "Logger.h"

int  Logger::mLoggerLevel = LOG_LEVEL_ERROR;

void Logger::setLoggerLevel(int level){
    mLoggerLevel = level;
}

void Logger::trace(const char* msg){
    if(mLoggerLevel <= LOG_LEVEL_TRACE){
        log("trace", msg);
    }
}

void Logger::debug(const char* msg){
    if(mLoggerLevel <= LOG_LEVEL_DEBUG){
        log("debug", msg);
    }
}

void Logger::info(const char* msg){
    if(mLoggerLevel <= LOG_LEVEL_INFO){
        log("info", msg);
    }
}

void Logger::warn(const char* msg){
    if(mLoggerLevel <= LOG_LEVEL_WARN){
        log("warn", msg);
    }
}

void Logger::error(const char* msg){
    if(mLoggerLevel <= LOG_LEVEL_ERROR){
        log("error", msg);
    }
}

void Logger::fatal(const char* msg){
    if(mLoggerLevel <= LOG_LEVEL_FATAL){
        log("fatal", msg);
    }
    exit(EXIT_FAILURE);
}

bool Logger::isTrace() {
    return mLoggerLevel ==  LOG_LEVEL_TRACE;
}

void Logger::log(const char* level, const char* msg){
    cout << " <" << level << "> " << msg << endl;
}

