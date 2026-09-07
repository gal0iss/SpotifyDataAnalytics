export interface MetricSummary {
  events: number;
  hours: number;
  tracks: number;
  artists: number;
  years: number;
}

export interface YearListening {
  year: number;
  events: number;
  hours: number;
}

export interface RankingRow {
  name: string;
  events: number;
  hours: number;
}

export interface DiscoverySummary {
  topTrack: string;
  topTrackEvents: number;
  topAlbum: string;
  topAlbumEvents: number;
  uniqueAlbums: number;
  diversity: number;
}

export interface GrowthRow {
  year: number;
  newArtists: number;
  newTracks: number;
  newAlbums: number;
}

export interface ListeningConsistency {
  peakDay: string;
  peakDayHours: number;
  peakMonth: string;
  peakMonthHours: number;
  activeDays: number;
  inactiveDays: number;
  maxStreak: number;
}

export interface HeatmapRow {
  weekday: string;
  hour: number;
  events: number;
}

export interface DeviceRow {
  device: string;
  events: number;
  hours: number;
  averageMinutes: number;
}

export interface CountryRow {
  country: string;
  events: number;
}

export interface FilterOptions {
  years: number[];
  devices: string[];
}

export interface Filters {
  year: string;
  device: string;
}

export interface InsightSummary {
  peakHour: number;
  peakHourEvents: number;
  activeWeekday: string;
  activeWeekdayEvents: number;
  averageMinutes: number;
  skipRate: number;
  shuffleRate: number;
}

export interface QualitySummary {
  unknownTracks: number;
  unknownLocations: number;
  trackCoverage: number;
  locationCoverage: number;
}
