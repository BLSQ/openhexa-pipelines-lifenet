QUERIES = {}

QUERIES["users"] = """
SELECT
  user_.id AS user_id,
  user_.firstname AS first_name,
  user_.lastname AS last_name,
  from_unixtime (user_.timecreated) AS time_created,
  from_unixtime (user_.lastaccess) AS last_access,
  cohort.idnumber AS org_unit,
  CAST(
    (
      CASE
        WHEN gender.data = 'unassigned' THEN NULL
        ELSE gender.data
      END
    ) AS CHAR(100)
  ) AS gender,
  CAST(
    (
      CASE
        WHEN POSITION.data = 'unassigned' THEN NULL
        ELSE POSITION.data
      END
    ) AS CHAR(100)
  ) AS 'position',
  CAST(
    (
      CASE
        WHEN phase_.data = 'unassigned' THEN NULL
        ELSE phase_.data
      END
    ) AS CHAR(100)
  ) AS 'phase'
FROM
  mdl_user AS user_
  LEFT JOIN mdl_cohort_members AS members ON user_.id = members.userid
  LEFT JOIN mdl_cohort AS cohort ON members.cohortid = cohort.id
  LEFT JOIN mdl_user_info_data AS gender ON user_.id = gender.userid
  AND gender.fieldid = 1
  LEFT JOIN mdl_user_info_data AS POSITION ON user_.id = POSITION.userid
  AND POSITION.fieldid = 2
  LEFT JOIN mdl_user_info_data AS phase_ ON user_.id = phase_.userid
  AND phase_.fieldid = 3
WHERE
  user_.deleted = 0
"""


QUERIES["grades"] = """
SELECT
  grade.userid AS user_id,
  gradeitem.courseid AS course_id,
  grade.rawgrade AS grade,
  grade.rawgrademax AS grade_max,
  ROUND((grade.rawgrade / grade.rawgrademax * 100), 2) AS score,
  gradeitem.itemname AS item_name,
  from_unixtime(grade.timemodified) as time_modified
FROM
  mdl_grade_grades AS grade
  LEFT JOIN mdl_grade_items AS gradeitem ON grade.itemid = gradeitem.id
WHERE
  grade.rawgrade IS NOT NULL
"""


QUERIES["courses"] = """
SELECT
  course.id AS course_id,
  course.fullname AS course_name,
  category.name AS MODULE,
  (
    CASE
      WHEN course.fullname LIKE '%-Med-%' THEN 'MEDICAL'
      WHEN course.fullname LIKE '%-Mgt-%' THEN 'MANAGEMENT'
      ELSE NULL
    END
  ) AS category
FROM
  mdl_course AS course
  LEFT JOIN mdl_course_categories AS category ON course.category = category.id
"""


QUERIES["enrollments"] = """
SELECT
  userenrol.userid AS user_id,
  enrol.courseid AS course_id,
  from_unixtime (userenrol.timecreated) AS enrollment_date
FROM
  mdl_user_enrolments AS userenrol
  LEFT JOIN mdl_enrol AS enrol ON userenrol.enrolid = enrol.id
"""


QUERIES["certificates"] = """
SELECT
  issues.userid AS user_id,
  template_.name AS certificate_name,
  from_unixtime (issues.timecreated) AS issue_date,
  issues.courseid AS course_id
FROM
  mdl_tool_certificate_issues AS issues
  LEFT JOIN mdl_tool_certificate_templates AS template_ ON issues.templateid = template_.id
"""


QUERIES["completions"] = """
SELECT
  completion.userid AS user_id,
  coursemodule.course AS course_id,
  completion.coursemoduleid AS course_module_id,
  quiz.name AS quiz_name,
  completion.completionstate AS completion_state,
  from_unixtime (completion.timemodified) AS time_modified
FROM
  mdl_course_modules_completion AS completion
  LEFT JOIN mdl_course_modules AS coursemodule ON completion.coursemoduleid = coursemodule.id
  LEFT JOIN mdl_quiz AS quiz ON coursemodule.instance = quiz.id
WHERE
  coursemodule.module = 18
"""
