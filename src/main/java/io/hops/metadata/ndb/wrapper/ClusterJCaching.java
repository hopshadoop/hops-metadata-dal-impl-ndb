package io.hops.metadata.ndb.wrapper;

public class ClusterJCaching {
  private final boolean useClusterjDtoCache;
  private final boolean useClusterjSessionCache;

  public ClusterJCaching(boolean useClusterjDtoCache, boolean useClusterjSessionCache) {
    this.useClusterjDtoCache = useClusterjDtoCache;
    this.useClusterjSessionCache = useClusterjSessionCache;
  }

  public boolean useClusterjDtoCache() {
    return useClusterjDtoCache;
  }

  public boolean useClusterjSessionCache() {
    return useClusterjSessionCache;
  }
}

