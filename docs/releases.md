# Releases

## Version 0.1.0
#### Release date: 28 February 2024
- Introduces default JSON draft version in case schema is lacking one
- Allows to force specific JSON draft and its corresponsing rules to be applied
- Introduces type handlers that support a specific set of JSON drafts 
- Introduces resolver registries 
- Allows to create own type handlers and define custom resolver registries
- Small improvents in resolving JSON types evolving over drafts. For instance Json arrays:
  
  In Draft 2020-12 `prefixItems` was introduced to allow tuple validation, meaning that when the array
  is a collection of items where each has a different schema and the ordinal index of each item is meaningful.   
  In Draft 4 - 2019-09, tuple validation was handled by an alternate form of the `items` keyword.
  When items was an array of schemas instead of a single schema, it behaved the way prefixItems behaves.

## Version 0.0.1
#### Release date: 11 December 2023
- Initial release supporting simple to moderate complex JSON structures
